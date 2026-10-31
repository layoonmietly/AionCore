use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};
use tracing::{error, info, warn};

use crate::action::{ActionExecutor, MessageResult};
use crate::error::ChannelError;
use crate::message_service::ChannelMessageService;
use crate::session::SessionManager;
use crate::stream_relay::{ChannelSender, ChannelStreamRelay, RelayConfig, throttle_ms_for_platform};
use crate::team_dispatch::ChannelTeamDispatcher;
use crate::types::{
    ActionBehavior, OutgoingMessageType, UnifiedAttachment, UnifiedIncomingMessage, UnifiedOutgoingMessage,
};

/// Per-chat delivery locks: chat id → serialization lock.
type DeliveryLocks = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

/// Orchestrates the full channel message lifecycle.
///
/// Consumes incoming IM messages from `message_rx` and tool confirmation
/// callbacks from `confirm_rx`, driving the pipeline:
/// 1. ActionExecutor routing (auth → action/AI dispatch)
/// 2. For Dispatched: send_to_agent + spawn ChannelStreamRelay
/// 3. For Action: reply via plugin
/// 4. Forward tool confirmations to the agent
pub struct ChannelOrchestrator {
    action_executor: Arc<ActionExecutor>,
    message_service: Arc<ChannelMessageService>,
    session_manager: Arc<SessionManager>,
    sender: Arc<dyn ChannelSender>,
    /// Dispatches messages for platforms bound to a team.
    team_dispatcher: Arc<dyn ChannelTeamDispatcher>,
    /// Per-chat delivery locks: a busy conversation queues the next message
    /// behind the active turn while preserving each chat's arrival order.
    delivery_locks: DeliveryLocks,
}

impl ChannelOrchestrator {
    pub fn new(
        action_executor: Arc<ActionExecutor>,
        message_service: Arc<ChannelMessageService>,
        session_manager: Arc<SessionManager>,
        sender: Arc<dyn ChannelSender>,
        team_dispatcher: Arc<dyn ChannelTeamDispatcher>,
    ) -> Self {
        Self {
            action_executor,
            message_service,
            session_manager,
            sender,
            team_dispatcher,
            delivery_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start the message loop. Runs until both channels close.
    pub async fn run(
        self,
        mut message_rx: mpsc::Receiver<UnifiedIncomingMessage>,
        mut confirm_rx: mpsc::Receiver<(String, String)>,
    ) {
        info!("ChannelOrchestrator started");

        loop {
            tokio::select! {
                Some(msg) = message_rx.recv() => {
                    self.handle_message(msg).await;
                }
                Some((call_id, value)) = confirm_rx.recv() => {
                    handle_confirm(&call_id, &value);
                }
                else => break,
            }
        }

        info!("ChannelOrchestrator stopped (channels closed)");
    }

    async fn handle_message(&self, msg: UnifiedIncomingMessage) {
        let platform = msg.platform;
        let chat_id = msg.chat_id.clone();
        let plugin_id = platform.to_string();
        let text = msg.content.text.clone();
        let attachments = msg.content.attachments.clone();
        let message_owner_user_id = msg
            .owner_user_id
            .clone()
            .or_else(|| self.action_executor.owner_user_id().map(ToOwned::to_owned));

        let executor = Arc::clone(&self.action_executor);
        let msg_svc = Arc::clone(&self.message_service);
        let session_mgr = Arc::clone(&self.session_manager);
        let sender = Arc::clone(&self.sender);
        let delivery_locks = Arc::clone(&self.delivery_locks);
        let team_dispatcher = Arc::clone(&self.team_dispatcher);

        tokio::spawn(async move {
            match executor.handle_incoming_message(&msg).await {
                Ok(MessageResult::Action(response)) => {
                    if let Some(owner_user_id) = message_owner_user_id.as_deref() {
                        send_action_response(&sender, owner_user_id, &plugin_id, &chat_id, &response).await;
                    } else {
                        warn!("dropping channel action response without owner user");
                    }
                }
                Ok(MessageResult::Dispatched {
                    owner_user_id,
                    session_id,
                    conversation_id,
                    text_override,
                }) => {
                    let dispatch_text = text_override.as_deref().unwrap_or(&text);
                    handle_dispatched(
                        &msg_svc,
                        &session_mgr,
                        &sender,
                        &delivery_locks,
                        &owner_user_id,
                        &session_id,
                        conversation_id.as_deref(),
                        dispatch_text,
                        attachments.as_deref(),
                        platform,
                        &plugin_id,
                        &chat_id,
                    )
                    .await;
                }
                Ok(MessageResult::TeamDispatched {
                    owner_user_id,
                    team_id,
                    text_override,
                }) => {
                    let dispatch_text = text_override.as_deref().unwrap_or(&text);
                    handle_team_dispatched(
                        &team_dispatcher,
                        &sender,
                        &delivery_locks,
                        &owner_user_id,
                        &team_id,
                        dispatch_text,
                        attachments.as_deref(),
                        platform,
                        &plugin_id,
                        &chat_id,
                    )
                    .await;
                }
                Ok(MessageResult::AlreadyProcessing) => {
                    info!(chat_id = %chat_id, "message ignored: already processing");
                }
                Err(e) => {
                    error!(error = %e, "failed to handle incoming message");
                }
            }
        });
    }
}

async fn send_action_response(
    sender: &Arc<dyn ChannelSender>,
    owner_user_id: &str,
    plugin_id: &str,
    chat_id: &str,
    response: &crate::types::ActionResponse,
) {
    if let Some(text) = &response.text {
        let outgoing = UnifiedOutgoingMessage {
            message_type: OutgoingMessageType::Text,
            text: Some(text.clone()),
            parse_mode: response.parse_mode,
            buttons: response.buttons.clone(),
            keyboard: response.keyboard.clone(),
            image_url: None,
            file_url: None,
            file_name: None,
            media_actions: None,
            reply_to_message_id: None,
            silent: None,
        };

        match response.behavior {
            ActionBehavior::Edit => {
                if let Some(ref edit_id) = response.edit_message_id {
                    let _ = sender
                        .edit_message(owner_user_id, plugin_id, chat_id, edit_id, outgoing)
                        .await;
                }
            }
            _ => {
                let _ = sender.send_message(owner_user_id, plugin_id, chat_id, outgoing).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_dispatched(
    msg_svc: &Arc<ChannelMessageService>,
    session_mgr: &Arc<SessionManager>,
    sender: &Arc<dyn ChannelSender>,
    delivery_locks: &DeliveryLocks,
    owner_user_id: &str,
    session_id: &str,
    conversation_id: Option<&str>,
    text: &str,
    attachments: Option<&[UnifiedAttachment]>,
    platform: crate::types::PluginType,
    plugin_id: &str,
    chat_id: &str,
) {
    let mut session = match session_mgr.get_session_by_id(owner_user_id, session_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            warn!(session_id = %session_id, "session not found after dispatch");
            return;
        }
        Err(e) => {
            error!(error = %e, "failed to get session");
            return;
        }
    };

    // Serialize deliveries per chat. This keeps attachment + follow-up text in
    // arrival order and prevents several queued messages from racing for the
    // same conversation when the active turn finishes.
    let chat_lock = {
        let mut locks = delivery_locks.lock().await;
        locks
            .entry(chat_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _chat_guard = chat_lock.lock().await;

    let mut queued_notice_sent = false;
    let send_result = loop {
        match msg_svc
            .send_to_agent(owner_user_id, &session, text, attachments, platform)
            .await
        {
            Ok(result) => break result,
            Err(ChannelError::ConversationBusy { conversation_id }) => {
                // `send_to_agent` may have created the conversation before it
                // discovered the active turn. Reuse that id on retry instead of
                // creating a second conversation for the same session.
                session.conversation_id = Some(conversation_id.clone());

                if !queued_notice_sent {
                    send_text_message(
                        sender,
                        owner_user_id,
                        plugin_id,
                        chat_id,
                        "\u{23f3} The assistant is still working. Your message is queued and will be processed automatically.",
                    )
                    .await;
                    queued_notice_sent = true;
                }

                if wait_until_conversation_idle(msg_svc, &conversation_id, Duration::from_secs(600))
                    .await
                    .is_err()
                {
                    send_text_message(
                        sender,
                        owner_user_id,
                        plugin_id,
                        chat_id,
                        "\u{274c} The queued message timed out while waiting for the assistant to finish.",
                    )
                    .await;
                    return;
                }
            }
            Err(e) => {
                error!(error = %e, "failed to send to agent");
                send_text_message(
                    sender,
                    owner_user_id,
                    plugin_id,
                    chat_id,
                    &format!("\u{274c} Failed to process: {e}"),
                )
                .await;
                return;
            }
        }
    };

    // Bind conversation to session if newly created
    if conversation_id.is_none()
        && let Err(e) = session_mgr
            .bind_conversation(owner_user_id, session_id, &send_result.conversation_id)
            .await
    {
        warn!(error = %e, "failed to bind conversation to session");
    }

    // Spawn stream relay if we got a subscription
    if let Some(rx) = send_result.stream_rx {
        let relay_config = RelayConfig {
            owner_user_id: owner_user_id.to_owned(),
            platform,
            plugin_id: plugin_id.to_owned(),
            chat_id: chat_id.to_owned(),
            throttle_ms: throttle_ms_for_platform(platform),
        };
        let relay = ChannelStreamRelay::new(relay_config, Arc::clone(sender));
        tokio::spawn(relay.run(rx));
    } else {
        warn!(
            conversation_id = %send_result.conversation_id,
            "no agent task for stream subscription"
        );
    }
}

/// Delivers a message to a team-bound platform and relays the leader's reply.
#[allow(clippy::too_many_arguments)]
async fn handle_team_dispatched(
    dispatcher: &Arc<dyn ChannelTeamDispatcher>,
    sender: &Arc<dyn ChannelSender>,
    delivery_locks: &DeliveryLocks,
    owner_user_id: &str,
    team_id: &str,
    text: &str,
    attachments: Option<&[UnifiedAttachment]>,
    platform: crate::types::PluginType,
    plugin_id: &str,
    chat_id: &str,
) {
    // Keep each chat's message order; team dispatch itself is enqueue-and-ack.
    let chat_lock = {
        let mut locks = delivery_locks.lock().await;
        locks
            .entry(chat_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _chat_guard = chat_lock.lock().await;

    let files = crate::message_service::chat_file_refs(attachments);
    match dispatcher.dispatch_to_team(owner_user_id, team_id, text, files).await {
        Ok(result) => {
            if let Some(rx) = result.stream_rx {
                let relay_config = RelayConfig {
                    owner_user_id: owner_user_id.to_owned(),
                    platform,
                    plugin_id: plugin_id.to_owned(),
                    chat_id: chat_id.to_owned(),
                    throttle_ms: throttle_ms_for_platform(platform),
                };
                let relay = ChannelStreamRelay::new(relay_config, Arc::clone(sender));
                tokio::spawn(relay.run(rx));
            } else {
                send_text_message(
                    sender,
                    owner_user_id,
                    plugin_id,
                    chat_id,
                    "\u{2705} Message delivered to the team. Its reply will arrive shortly.",
                )
                .await;
            }
        }
        Err(e) => {
            error!(error = %e, team_id, "failed to dispatch channel message to team");
            send_text_message(
                sender,
                owner_user_id,
                plugin_id,
                chat_id,
                &format!("\u{274c} Failed to send to the team: {e}"),
            )
            .await;
        }
    }
}

/// Wait until the conversation's active turn has released its claim.
async fn wait_until_conversation_idle(
    msg_svc: &Arc<ChannelMessageService>,
    conversation_id: &str,
    timeout: Duration,
) -> Result<(), ()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !msg_svc.is_conversation_busy(conversation_id) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn send_text_message(
    sender: &Arc<dyn ChannelSender>,
    owner_user_id: &str,
    plugin_id: &str,
    chat_id: &str,
    text: &str,
) {
    let message = UnifiedOutgoingMessage {
        message_type: OutgoingMessageType::Text,
        text: Some(text.to_owned()),
        parse_mode: None,
        buttons: None,
        keyboard: None,
        image_url: None,
        file_url: None,
        file_name: None,
        media_actions: None,
        reply_to_message_id: None,
        silent: None,
    };
    let _ = sender.send_message(owner_user_id, plugin_id, chat_id, message).await;
}

/// Forward a tool confirmation callback to the active agent.
fn handle_confirm(call_id: &str, value: &str) {
    // Channel conversations use yoloMode which auto-approves everything,
    // so this path is rarely hit. When needed, we can add a
    // call_id→conversation_id lookup via IWorkerTaskManager.
    info!(call_id = %call_id, value = %value, "forwarding tool confirmation");
}
