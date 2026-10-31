use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::constants::{LARK_EVENT_DEDUP_TTL, LARK_MESSAGE_LIMIT};
use crate::error::ChannelError;
use crate::plugin::{ChannelPlugin, PluginCallbacks};
use crate::types::{
    ActionCategory, ActionContext, BotInfo, MessageContentType, PluginConfig, PluginStatus, PluginType, UnifiedAction,
    UnifiedAttachment, UnifiedIncomingMessage, UnifiedMessageContent, UnifiedOutgoingMessage, UnifiedUser,
};

use super::api::{DownloadedResource, LarkApi};
use super::frame::{
    METHOD_CONTROL, METHOD_DATA, build_ack_frame, build_card_response_frame, build_ping_frame, decode_frame,
    encode_frame, get_header,
};
use super::types::{BotMenuEvent, CardActionEvent, MessageEvent, build_interactive_card, parse_lark_callback};
use super::types::{PostContent, PostElement, QuotedMessageItem};
use super::ws_session::{FragmentCache, parse_pong_config};

/// Maximum reconnect attempts before giving up.
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Maximum backoff delay between reconnection attempts.
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

/// Interval between event dedup cache cleanup sweeps.
const DEDUP_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Ignore message events older than this. Lark redelivers events that were not
/// acknowledged before a restart, and answering one long afterwards looks like
/// the bot replying on its own to a conversation the user already moved past.
const MAX_EVENT_AGE: Duration = Duration::from_secs(10 * 60);

/// File that keeps recent event ids across process restarts.
const LARK_EVENT_DEDUP_FILE: &str = "event_dedup.json";

/// Lark (Feishu) platform plugin.
///
/// Connects via WebSocket long connection, handles message events,
/// card action triggers, and bot menu events. All responses use
/// interactive cards (Lark only supports editing card messages).
pub struct LarkPlugin {
    status: PluginStatus,
    bot_info: Option<BotInfo>,
    last_error: Option<String>,
    api: Option<Arc<LarkApi>>,
    callbacks: Option<PluginCallbacks>,
    ws_handle: Option<JoinHandle<()>>,
    cleanup_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    /// Shared event deduplication cache: event_id → received_at.
    dedup_cache: Arc<Mutex<HashMap<String, Instant>>>,
}

impl Default for LarkPlugin {
    fn default() -> Self {
        Self {
            status: PluginStatus::Created,
            bot_info: None,
            last_error: None,
            api: None,
            callbacks: None,
            ws_handle: None,
            cleanup_handle: None,
            shutdown_tx: None,
            dedup_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl LarkPlugin {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ChannelPlugin for LarkPlugin {
    async fn initialize(&mut self, config: PluginConfig, callbacks: PluginCallbacks) -> Result<(), ChannelError> {
        self.status = PluginStatus::Initializing;

        let app_id = config
            .credentials
            .app_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                self.status = PluginStatus::Error;
                self.last_error = Some("Missing Lark app_id".into());
                ChannelError::InvalidConfig("Missing Lark app_id".into())
            })?;

        let app_secret = config
            .credentials
            .app_secret
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                self.status = PluginStatus::Error;
                self.last_error = Some("Missing Lark app_secret".into());
                ChannelError::InvalidConfig("Missing Lark app_secret".into())
            })?;

        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| {
                self.status = PluginStatus::Error;
                self.last_error = Some(format!("HTTP client init failed: {e}"));
                ChannelError::ConnectionFailed(format!("HTTP client init failed: {e}"))
            })?;

        let api = Arc::new(LarkApi::new(http_client, app_id, app_secret));

        // Validate credentials by getting bot info
        let bot_data = api.get_bot_info().await.map_err(|e| {
            self.status = PluginStatus::Error;
            self.last_error = Some(format!("Credential validation failed: {e}"));
            e
        })?;

        self.bot_info = Some(BotInfo {
            id: bot_data.open_id.clone(),
            username: None,
            display_name: bot_data.app_name.clone(),
        });

        info!(
            bot_name = bot_data.app_name,
            bot_id = bot_data.open_id,
            "Lark bot initialized"
        );

        // Restore recently handled event ids so Lark's post-restart retries
        // cannot re-answer messages that were already processed.
        self.dedup_cache = Arc::new(Mutex::new(load_persisted_events()));

        self.api = Some(api);
        self.callbacks = Some(callbacks);
        self.status = PluginStatus::Ready;
        Ok(())
    }

    async fn start(&mut self) -> Result<(), ChannelError> {
        self.status = PluginStatus::Starting;

        let callbacks = self.callbacks.take().ok_or_else(|| {
            self.status = PluginStatus::Error;
            ChannelError::ConnectionFailed("Plugin not initialized".into())
        })?;

        // Set up shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Spawn the WebSocket connection loop
        let api_clone = Arc::clone(self.api.as_ref().expect("api set in initialize"));
        let dedup_cache = Arc::clone(&self.dedup_cache);
        self.ws_handle = Some(tokio::spawn(ws_loop(
            api_clone,
            callbacks.message_tx,
            callbacks.confirm_tx,
            dedup_cache.clone(),
            shutdown_rx,
        )));

        // Spawn the dedup cache cleanup task
        let dedup_for_cleanup = dedup_cache;
        let mut cleanup_shutdown = self.shutdown_tx.as_ref().expect("shutdown_tx just set").subscribe();
        self.cleanup_handle = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(DEDUP_CLEANUP_INTERVAL) => {
                        cleanup_expired_events(&dedup_for_cleanup).await;
                    }
                    _ = cleanup_shutdown.changed() => {
                        break;
                    }
                }
            }
        }));

        self.status = PluginStatus::Running;
        info!("Lark plugin started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ChannelError> {
        self.status = PluginStatus::Stopping;

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        if let Some(handle) = self.ws_handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        if let Some(handle) = self.cleanup_handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }

        self.api = None;
        self.status = PluginStatus::Stopped;
        info!("Lark plugin stopped");
        Ok(())
    }

    async fn send_message(&self, chat_id: &str, message: UnifiedOutgoingMessage) -> Result<String, ChannelError> {
        let api = self
            .api
            .as_ref()
            .ok_or_else(|| ChannelError::PlatformApi("Plugin not initialized".into()))?;

        let text = truncate_message(message.text.as_deref().unwrap_or(""), LARK_MESSAGE_LIMIT);

        let card_content = build_interactive_card(&text, message.buttons.as_deref());
        let data = api.send_card(chat_id, &card_content).await?;
        Ok(data.message_id)
    }

    async fn edit_message(
        &self,
        _chat_id: &str,
        message_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<(), ChannelError> {
        let api = self
            .api
            .as_ref()
            .ok_or_else(|| ChannelError::PlatformApi("Plugin not initialized".into()))?;

        let text = truncate_message(message.text.as_deref().unwrap_or(""), LARK_MESSAGE_LIMIT);

        let card_content = build_interactive_card(&text, message.buttons.as_deref());
        api.update_card(message_id, &card_content).await
    }

    fn active_user_count(&self) -> usize {
        0
    }

    fn bot_info(&self) -> Option<&BotInfo> {
        self.bot_info.as_ref()
    }

    fn plugin_type(&self) -> PluginType {
        PluginType::Lark
    }

    fn status(&self) -> PluginStatus {
        self.status
    }

    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

// ---------------------------------------------------------------------------
// WebSocket connection loop
// ---------------------------------------------------------------------------

/// Background task that maintains a WebSocket connection to Lark.
///
/// On disconnect, implements exponential backoff reconnection up to
/// `MAX_RECONNECT_ATTEMPTS`.
async fn ws_loop(
    api: Arc<LarkApi>,
    message_tx: mpsc::Sender<UnifiedIncomingMessage>,
    confirm_tx: mpsc::Sender<(String, String)>,
    dedup_cache: Arc<Mutex<HashMap<String, Instant>>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut consecutive_errors: u32 = 0;

    loop {
        if *shutdown_rx.borrow() {
            debug!("Lark WS loop received shutdown signal");
            break;
        }

        let endpoint_data = match api.get_ws_endpoint().await {
            Ok(data) => data,
            Err(e) => {
                consecutive_errors += 1;
                warn!(error = %e, consecutive_errors, "Lark WS endpoint fetch failed");
                if consecutive_errors >= MAX_RECONNECT_ATTEMPTS {
                    error!("Lark max reconnect attempts reached");
                    break;
                }
                let delay = backoff_delay(consecutive_errors);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => continue,
                    _ = shutdown_rx.changed() => break,
                }
            }
        };

        let ws_url = &endpoint_data.url;
        let initial_ping_secs = endpoint_data
            .client_config
            .as_ref()
            .and_then(|c| c.ping_interval)
            .unwrap_or(120);

        let service_id = extract_service_id(ws_url);
        debug!(url = %ws_url, service_id, initial_ping_secs, "Connecting to Lark WebSocket");

        match connect_and_listen(
            ws_url,
            service_id,
            initial_ping_secs,
            &message_tx,
            &confirm_tx,
            &dedup_cache,
            &api,
            &mut shutdown_rx,
        )
        .await
        {
            Ok(()) => {
                debug!("Lark WS connection closed cleanly");
                break;
            }
            Err(e) => {
                consecutive_errors += 1;
                warn!(error = %e, consecutive_errors, "Lark WS connection error");
                if consecutive_errors >= MAX_RECONNECT_ATTEMPTS {
                    error!("Lark max reconnect attempts reached");
                    break;
                }
                let delay = backoff_delay(consecutive_errors);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown_rx.changed() => break,
                }
            }
        }
    }

    debug!("Lark WS loop exited");
}

/// Connect to the WebSocket and listen for binary protobuf frames.
#[allow(clippy::too_many_arguments)]
async fn connect_and_listen(
    ws_url: &str,
    service_id: i32,
    initial_ping_secs: u64,
    message_tx: &mpsc::Sender<UnifiedIncomingMessage>,
    confirm_tx: &mpsc::Sender<(String, String)>,
    dedup_cache: &Arc<Mutex<HashMap<String, Instant>>>,
    api: &Arc<LarkApi>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), ChannelError> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async_tls_with_config;
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    let connector = build_ws_tls_connector()?;
    let (ws_stream, _) = connect_async_tls_with_config(ws_url, None, false, Some(connector))
        .await
        .map_err(|e| ChannelError::ConnectionFailed(format!("Lark WS connect failed: {e}")))?;

    info!("Lark WebSocket connected");

    let (mut write, mut read) = ws_stream.split();
    let mut fragment_cache = FragmentCache::new();
    let mut ping_duration = Duration::from_secs(initial_ping_secs);
    let mut ping_deadline = tokio::time::Instant::now() + ping_duration;

    // Event handling (attachment downloads, quoted-message lookups) can take
    // longer than Lark's acknowledgement window. Ack on the read loop and
    // process payloads here, one at a time, so arrival order is preserved and
    // slow handling never delays pings or the next event's ack.
    let (handler_tx, mut handler_rx) = mpsc::channel::<(String, String)>(64);
    let worker_message_tx = message_tx.clone();
    let worker_confirm_tx = confirm_tx.clone();
    let worker_dedup = Arc::clone(dedup_cache);
    let worker_api = Arc::clone(api);
    tokio::spawn(async move {
        while let Some((text, msg_type)) = handler_rx.recv().await {
            handle_ws_text(
                &text,
                &msg_type,
                &worker_message_tx,
                &worker_confirm_tx,
                &worker_dedup,
                Some(&worker_api),
            )
            .await;
        }
    });

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(WsMessage::Binary(data))) => {
                        let frame = match decode_frame(&data) {
                            Ok(f) => f,
                            Err(e) => {
                                warn!(error = %e, "Failed to decode Lark protobuf frame");
                                continue;
                            }
                        };

                        match frame.method {
                            METHOD_CONTROL => {
                                if let Some(new_duration) = handle_control_frame(&frame) {
                                    ping_duration = new_duration;
                                    ping_deadline = tokio::time::Instant::now() + ping_duration;
                                }
                            }
                            METHOD_DATA => {
                                let message_id = get_header(&frame.headers, "message_id")
                                    .unwrap_or("")
                                    .to_owned();
                                let sum: usize = get_header(&frame.headers, "sum")
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(1);
                                let seq: usize = get_header(&frame.headers, "seq")
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(0);

                                if let Some(merged) = fragment_cache.push(&message_id, sum, seq, &frame.payload) {
                                    let msg_type = get_header(&frame.headers, "type").unwrap_or("").to_owned();
                                    let payload = if msg_type == "event" || msg_type == "card" {
                                        match String::from_utf8(merged) {
                                            Ok(text) => Some(text),
                                            Err(_) => {
                                                warn!("Lark frame payload is not valid UTF-8");
                                                None
                                            }
                                        }
                                    } else {
                                        None
                                    };

                                    // Ack *before* handling. Lark retries events
                                    // that are not acknowledged within a few
                                    // seconds; handling includes network downloads,
                                    // so acking afterwards makes Feishu redeliver
                                    // the same message and the bot answers twice.
                                    // Card callbacks need a `data` payload, but the
                                    // real response is sent asynchronously via card
                                    // sends/updates, so an immediate toast suffices.
                                    let card_action = payload
                                        .as_deref()
                                        .is_some_and(|text| is_card_action_payload(text, &msg_type));
                                    let response_frame = if card_action {
                                        build_card_response_frame(&frame, &card_action_response())
                                    } else {
                                        build_ack_frame(&frame)
                                    };
                                    let response_bytes = encode_frame(&response_frame);
                                    if let Err(e) = write.send(WsMessage::Binary(response_bytes.into())).await {
                                        warn!(error = %e, "Failed to send Lark event response");
                                    }

                                    if let Some(text) = payload
                                        && handler_tx.send((text, msg_type)).await.is_err()
                                    {
                                        warn!("Lark event handler queue closed");
                                    }
                                }
                            }
                            _ => {
                                debug!(method = frame.method, "Lark unknown frame method");
                            }
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) => {
                        debug!("Lark WS received close frame");
                        return Ok(());
                    }
                    Some(Err(e)) => {
                        return Err(ChannelError::ConnectionFailed(
                            format!("Lark WS read error: {e}")
                        ));
                    }
                    None => {
                        return Err(ChannelError::ConnectionFailed(
                            "Lark WS stream ended unexpectedly".into()
                        ));
                    }
                    _ => {}
                }
            }
            _ = tokio::time::sleep_until(ping_deadline) => {
                let ping = build_ping_frame(service_id);
                let ping_bytes = encode_frame(&ping);
                if let Err(e) = write.send(WsMessage::Binary(ping_bytes.into())).await {
                    warn!(error = %e, "Failed to send Lark ping frame");
                    return Err(ChannelError::ConnectionFailed("Lark ping send failed".into()));
                }
                debug!("Lark ping sent");
                ping_deadline = tokio::time::Instant::now() + ping_duration;
                fragment_cache.cleanup(Duration::from_secs(300));
            }
            _ = shutdown_rx.changed() => {
                debug!("Lark WS shutdown during listen");
                return Ok(());
            }
        }
    }
}

/// Handle a control frame (ping/pong). Returns updated ping duration if pong contains config.
fn handle_control_frame(frame: &super::frame::PbFrame) -> Option<Duration> {
    let frame_type = get_header(&frame.headers, "type").unwrap_or("");
    match frame_type {
        "pong" => {
            if !frame.payload.is_empty()
                && let Some(config) = parse_pong_config(&frame.payload)
            {
                debug!(
                    interval_secs = config.ping_interval_secs,
                    "Lark ping interval updated from pong"
                );
                return Some(Duration::from_secs(config.ping_interval_secs));
            }
            None
        }
        "ping" => None,
        _ => {
            debug!(frame_type, "Lark unknown control frame type");
            None
        }
    }
}

/// Handle a decoded and reassembled Lark event payload.
///
/// Returns a card-action response JSON when the event was a card action. The
/// WebSocket read loop acks immediately and calls this from an ordered worker
/// task, so the return value is informational; it is also what the handler
/// tests assert on. Non-card events return `None`.
async fn handle_ws_text(
    text: &str,
    frame_type: &str,
    message_tx: &mpsc::Sender<UnifiedIncomingMessage>,
    confirm_tx: &mpsc::Sender<(String, String)>,
    dedup_cache: &Arc<Mutex<HashMap<String, Instant>>>,
    api: Option<&Arc<LarkApi>>,
) -> Option<String> {
    match frame_type {
        "event" => {
            let envelope: serde_json::Value = match serde_json::from_str(text) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "Failed to parse Lark event JSON");
                    return None;
                }
            };

            let event_id = envelope
                .get("header")
                .and_then(|h| h.get("event_id"))
                .and_then(|v| v.as_str());
            if let Some(eid) = event_id
                && is_duplicate(dedup_cache, eid).await
            {
                debug!(event_id = eid, "Lark duplicate event, skipping");
                return None;
            }

            let event_type = envelope
                .get("header")
                .and_then(|h| h.get("event_type"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            match event_type {
                "im.message.receive_v1" => {
                    if let Some(event_data) = envelope.get("event").cloned() {
                        handle_message_event(event_data, message_tx, api).await;
                    }
                    None
                }
                "application.bot.menu_v6" => {
                    if let Some(event_data) = envelope.get("event").cloned() {
                        handle_bot_menu_event(event_data, message_tx).await;
                    }
                    None
                }
                "card.action.trigger" => {
                    if let Some(event_data) = envelope.get("event").cloned() {
                        handle_card_action(event_data, message_tx, confirm_tx).await;
                    }
                    Some(card_action_response())
                }
                _ => {
                    debug!(event_type, "Lark unhandled event type");
                    None
                }
            }
        }
        "card" => {
            let data: serde_json::Value = match serde_json::from_str(text) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "Failed to parse Lark card action JSON");
                    return None;
                }
            };
            handle_card_action(data, message_tx, confirm_tx).await;
            Some(card_action_response())
        }
        _ => {
            debug!(frame_type, "Lark unhandled payload type");
            None
        }
    }
}

/// Whether a WS payload is a card action callback, which must be answered
/// with a response frame carrying a business `data` payload.
fn is_card_action_payload(text: &str, frame_type: &str) -> bool {
    if frame_type == "card" {
        return true;
    }
    if frame_type != "event" {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("header")
                .and_then(|header| header.get("event_type"))
                .and_then(|event_type| event_type.as_str())
                .map(str::to_owned)
        })
        .is_some_and(|event_type| event_type == "card.action.trigger")
}

/// Immediate toast returned to Feishu for a card action. The real response
/// (new session card, regenerated reply, etc.) is sent asynchronously by the
/// orchestrator, so the callback itself only needs an acknowledgement.
fn card_action_response() -> String {
    serde_json::json!({
        "toast": {
            "type": "info",
            "content": "Processing..."
        }
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Event handlers
// ---------------------------------------------------------------------------

/// Handle an `im.message.receive_v1` event.
async fn handle_message_event(
    event_data: serde_json::Value,
    message_tx: &mpsc::Sender<UnifiedIncomingMessage>,
    api: Option<&Arc<LarkApi>>,
) {
    let evt: MessageEvent = match serde_json::from_value(event_data.clone()) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, raw = %event_data, "Failed to parse Lark message event");
            return;
        }
    };

    if let Some(age_secs) = stale_event_age(evt.message.create_time.as_deref()) {
        info!(
            message_id = %evt.message.message_id,
            age_secs,
            "skipping stale Lark message event"
        );
        return;
    }

    let user = UnifiedUser {
        id: evt.sender.sender_id.open_id.clone(),
        username: None,
        display_name: evt.sender.sender_id.open_id.clone(),
        avatar_url: None,
    };

    let (content_type, mut text, mut attachments) = extract_message_content(
        &evt.message.message_type,
        &evt.message.content,
        evt.message.mentions.as_deref(),
    );

    // Feishu quote/reply only carries the parent message id. Fetch the quoted
    // message so the agent sees the context instead of just the new text.
    // Quoted attachments are resolved after the current message's own
    // attachments, because they belong to a different message id.
    let mut quoted_attachments = None;
    let mut quoted_attachment_message_id = String::new();
    if !matches!(&content_type, MessageContentType::Command)
        && let Some(parent_id) = evt.message.parent_id.as_deref().filter(|id| !id.is_empty())
        && let Some(api) = api
    {
        match api.get_message(parent_id).await {
            Ok(quoted) => {
                debug!(
                    parent_id,
                    quoted_message_id = %quoted.message_id,
                    "loaded quoted Lark message"
                );
                if let Some(quoted_text) = quoted_message_text(&quoted) {
                    text = if text.trim().is_empty() {
                        format!("[Quoted message]\n{quoted_text}")
                    } else {
                        format!("[Quoted message]\n{quoted_text}\n\n{text}")
                    };
                }

                // Text alone is not enough when the quoted message is an image
                // or file: those live behind the resource API and are not in
                // the current message's payload. Download them so the agent can
                // actually inspect what was quoted, even when the quoted
                // message is outside the recent conversation context.
                let quoted_content = quoted.body.as_ref().map(|body| body.content.as_str()).unwrap_or("");
                let (_, _, extracted) = extract_message_content(&quoted.msg_type, quoted_content, None);
                if extracted.is_some() {
                    quoted_attachment_message_id = if quoted.message_id.is_empty() {
                        parent_id.to_owned()
                    } else {
                        quoted.message_id.clone()
                    };
                    quoted_attachments = extracted;
                }
            }
            Err(err) => warn!(error = %err, parent_id, "failed to load quoted Lark message"),
        }
    }

    // Lark message resources (images/files/audio) are addressable only through
    // the message-resource API, so fetch them while we still have both the
    // message id and the resource key. Downloads that fail leave `local_path`
    // unset; the orchestrator then reports that the attachment was unavailable
    // instead of silently sending an empty message.
    if let (Some(items), Some(api)) = (attachments.as_mut(), api) {
        download_lark_attachments(api, &evt.message.message_id, items).await;
    }

    if let (Some(mut items), Some(api)) = (quoted_attachments, api) {
        download_lark_attachments(api, &quoted_attachment_message_id, &mut items).await;
        attachments.get_or_insert_with(Vec::new).extend(items);
    }

    let timestamp = evt
        .message
        .create_time
        .as_deref()
        .and_then(|t| t.parse::<i64>().ok())
        .map(|ms| ms / 1000)
        .unwrap_or_else(chrono_now);

    let unified = UnifiedIncomingMessage {
        owner_user_id: None,
        id: evt.message.message_id.clone(),
        platform: PluginType::Lark,
        chat_id: evt.message.chat_id.clone(),
        user,
        content: UnifiedMessageContent {
            content_type,
            text,
            attachments,
        },
        timestamp,
        reply_to_message_id: evt.message.parent_id.clone(),
        action: None,
        raw: None,
    };

    let _ = message_tx.send(unified).await;
}

/// Handle a `card.action.trigger` event.
async fn handle_card_action(
    data: serde_json::Value,
    message_tx: &mpsc::Sender<UnifiedIncomingMessage>,
    confirm_tx: &mpsc::Sender<(String, String)>,
) {
    let evt: CardActionEvent = match serde_json::from_value(data) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "Failed to parse Lark card action");
            return;
        }
    };

    // Extract action string from the card button value
    let action_str = evt
        .action
        .value
        .as_ref()
        .and_then(|v| v.get("action"))
        .and_then(|a| a.as_str())
        .unwrap_or("");

    let parsed = parse_lark_callback(action_str);

    // Check if this is a tool confirmation
    if let Some((_, ref action, ref params)) = parsed
        && action == "system.confirm"
        && let Some(p) = params
    {
        let call_id = p.get("callId").cloned().unwrap_or_default();
        let value = p.get("value").cloned().unwrap_or_default();
        if !call_id.is_empty() {
            let _ = confirm_tx.send((call_id, value)).await;
        }
    }

    let chat_id = evt
        .context
        .as_ref()
        .and_then(|context| context.open_chat_id.clone())
        .or_else(|| evt.open_chat_id.clone())
        .unwrap_or_default();

    let message_id = evt
        .context
        .as_ref()
        .and_then(|context| context.open_message_id.clone())
        .or_else(|| evt.open_message_id.clone());

    info!(
        action = action_str,
        chat_id = %chat_id,
        message_id = message_id.as_deref().unwrap_or(""),
        "Lark card action received"
    );

    let user = UnifiedUser {
        id: evt.operator.open_id.clone(),
        username: None,
        display_name: evt.operator.open_id.clone(),
        avatar_url: None,
    };

    let unified_action = parsed.map(|(cat_str, action, params)| {
        let category = match cat_str.as_str() {
            "platform" => ActionCategory::Platform,
            "chat" => ActionCategory::Chat,
            _ => ActionCategory::System,
        };
        UnifiedAction {
            action,
            category,
            params,
            context: ActionContext {
                platform: PluginType::Lark,
                user_id: evt.operator.open_id.clone(),
                chat_id: chat_id.clone(),
                message_id: message_id.clone(),
                session_id: None,
            },
        }
    });

    let msg = UnifiedIncomingMessage {
        owner_user_id: None,
        id: format!("card_{}", chrono_now()),
        platform: PluginType::Lark,
        chat_id,
        user,
        content: UnifiedMessageContent {
            content_type: MessageContentType::Action,
            text: action_str.to_string(),
            attachments: None,
        },
        timestamp: chrono_now(),
        reply_to_message_id: None,
        action: unified_action,
        raw: None,
    };

    let _ = message_tx.send(msg).await;
}

/// Handle an `application.bot.menu_v6` event.
async fn handle_bot_menu_event(event_data: serde_json::Value, message_tx: &mpsc::Sender<UnifiedIncomingMessage>) {
    let evt: BotMenuEvent = match serde_json::from_value(event_data) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "Failed to parse Lark bot menu event");
            return;
        }
    };

    let user = UnifiedUser {
        id: evt.operator.operator_id.open_id.clone(),
        username: None,
        display_name: evt.operator.operator_id.open_id.clone(),
        avatar_url: None,
    };

    let msg = UnifiedIncomingMessage {
        owner_user_id: None,
        id: format!("menu_{}", chrono_now()),
        platform: PluginType::Lark,
        chat_id: evt.operator.operator_id.open_id.clone(),
        user,
        content: UnifiedMessageContent {
            content_type: MessageContentType::Command,
            text: format!("/{}", evt.event_key),
            attachments: None,
        },
        timestamp: chrono_now(),
        reply_to_message_id: None,
        action: None,
        raw: None,
    };

    let _ = message_tx.send(msg).await;
}

// ---------------------------------------------------------------------------
// Message content extraction
// ---------------------------------------------------------------------------

/// Extract content type, text, and attachments from a Lark message.
///
/// The `content` field is a JSON string; the structure depends on
/// `message_type` (text, image, file, audio, etc.).
fn extract_message_content(
    message_type: &str,
    content_json: &str,
    mentions: Option<&[super::types::Mention]>,
) -> (MessageContentType, String, Option<Vec<UnifiedAttachment>>) {
    match message_type {
        "text" => {
            let mut text = serde_json::from_str::<super::types::TextContent>(content_json)
                .map(|tc| tc.text)
                .unwrap_or_default();

            // Strip mention placeholders like @_user_1
            if let Some(ms) = mentions {
                for m in ms {
                    text = text.replace(&m.key, "").trim().to_string();
                }
            }

            if text.starts_with('/') {
                (MessageContentType::Command, text, None)
            } else {
                (MessageContentType::Text, text, None)
            }
        }
        "image" => {
            let image_key = serde_json::from_str::<super::types::ImageContent>(content_json)
                .map(|ic| ic.image_key)
                .unwrap_or_default();
            let attachments = vec![UnifiedAttachment {
                file_id: Some(image_key),
                file_name: None,
                mime_type: Some("image/jpeg".into()),
                file_size: None,
                url: None,
                local_path: None,
            }];
            (MessageContentType::Photo, String::new(), Some(attachments))
        }
        "file" => {
            let fc = serde_json::from_str::<super::types::FileContent>(content_json);
            let (file_key, file_name) = fc.map(|f| (f.file_key, f.file_name)).unwrap_or_default();
            let attachments = vec![UnifiedAttachment {
                file_id: Some(file_key),
                file_name,
                mime_type: None,
                file_size: None,
                url: None,
                local_path: None,
            }];
            (MessageContentType::Document, String::new(), Some(attachments))
        }
        "audio" => {
            let file_key = serde_json::from_str::<super::types::AudioContent>(content_json)
                .map(|ac| ac.file_key)
                .unwrap_or_default();
            let attachments = vec![UnifiedAttachment {
                file_id: Some(file_key),
                file_name: None,
                mime_type: Some("audio/opus".into()),
                file_size: None,
                url: None,
                local_path: None,
            }];
            (MessageContentType::Audio, String::new(), Some(attachments))
        }
        "post" => extract_post_content(content_json).unwrap_or_else(|| {
            warn!(content = %content_json, "Failed to parse Lark rich-text message");
            (
                MessageContentType::Text,
                "[Unsupported rich text message]".to_owned(),
                None,
            )
        }),
        _ => {
            // Unsupported type — treat as text with the raw JSON
            (
                MessageContentType::Text,
                format!("[Unsupported message type: {message_type}]"),
                None,
            )
        }
    }
}

/// Convert a Lark rich-text (`post`) message into plain text + attachments.
///
/// Text-ish elements are flattened into lines; `img`/`media` elements become
/// attachments carrying their resource keys, which the caller downloads before
/// dispatch. Returns `None` when the payload does not match either the flat or
/// the locale-keyed post shape.
fn extract_post_content(content_json: &str) -> Option<(MessageContentType, String, Option<Vec<UnifiedAttachment>>)> {
    let post = parse_post_content(content_json)?;
    let mut lines: Vec<String> = Vec::new();
    let mut attachments: Vec<UnifiedAttachment> = Vec::new();
    let mut has_image = false;

    for line in &post.content {
        let mut line_text = String::new();
        for element in line {
            match element {
                PostElement::Text { text } => line_text.push_str(text),
                PostElement::Link { text, href } => {
                    if text.is_empty() {
                        line_text.push_str(href);
                    } else {
                        line_text.push_str(text);
                        if !href.is_empty() {
                            line_text.push_str(" (");
                            line_text.push_str(href);
                            line_text.push(')');
                        }
                    }
                }
                PostElement::At { user_id, user_name } => {
                    line_text.push('@');
                    line_text.push_str(if user_name.is_empty() { user_id } else { user_name });
                }
                PostElement::Img { image_key } if !image_key.is_empty() => {
                    attachments.push(image_attachment(image_key));
                    has_image = true;
                }
                PostElement::Media {
                    file_key,
                    image_key,
                    file_name,
                    mime_type,
                } => {
                    if !file_key.is_empty() {
                        attachments.push(UnifiedAttachment {
                            file_id: Some(file_key.clone()),
                            file_name: (!file_name.is_empty()).then(|| file_name.clone()),
                            mime_type: mime_type.clone(),
                            file_size: None,
                            url: None,
                            local_path: None,
                        });
                    } else if !image_key.is_empty() {
                        attachments.push(image_attachment(image_key));
                        has_image = true;
                    }
                }
                PostElement::Emotion { emoji_type } => {
                    line_text.push('[');
                    line_text.push_str(emoji_type);
                    line_text.push(']');
                }
                PostElement::CodeBlock { language, text } => {
                    if !line_text.is_empty() {
                        lines.push(std::mem::take(&mut line_text));
                    }
                    if language.is_empty() {
                        lines.push(format!("```\n{text}\n```"));
                    } else {
                        lines.push(format!("```{language}\n{text}\n```"));
                    }
                }
                PostElement::Markdown { text } => line_text.push_str(text),
                PostElement::Hr => {
                    if !line_text.is_empty() {
                        lines.push(std::mem::take(&mut line_text));
                    }
                    lines.push("---".to_owned());
                }
                PostElement::Img { .. } | PostElement::Unknown => {}
            }
        }
        if !line_text.trim().is_empty() {
            lines.push(line_text);
        }
    }

    let mut text = String::new();
    if !post.title.trim().is_empty() {
        text.push_str(post.title.trim());
        text.push_str("\n\n");
    }
    text.push_str(&lines.join("\n"));
    let text = text.trim().to_owned();

    let content_type = if text.starts_with('/') {
        MessageContentType::Command
    } else if !text.is_empty() {
        MessageContentType::Text
    } else if has_image {
        MessageContentType::Photo
    } else if !attachments.is_empty() {
        MessageContentType::Document
    } else {
        MessageContentType::Text
    };

    Some((content_type, text, (!attachments.is_empty()).then_some(attachments)))
}

fn image_attachment(image_key: &str) -> UnifiedAttachment {
    UnifiedAttachment {
        file_id: Some(image_key.to_owned()),
        file_name: None,
        mime_type: Some("image/jpeg".into()),
        file_size: None,
        url: None,
        local_path: None,
    }
}

fn parse_post_content(content_json: &str) -> Option<PostContent> {
    if let Ok(post) = serde_json::from_str::<PostContent>(content_json)
        && (!post.title.is_empty() || !post.content.is_empty())
    {
        return Some(post);
    }

    let localized: HashMap<String, PostContent> = serde_json::from_str(content_json).ok()?;
    localized
        .get("zh_cn")
        .or_else(|| localized.get("en_us"))
        .or_else(|| localized.values().next())
        .cloned()
}

/// Extract readable text from a quoted Lark message.
///
/// Feishu returns a fetched message's content as a JSON string whose shape
/// depends on `msg_type`; interactive cards (the bot's own replies) carry the
/// text in nested `markdown` elements.
fn quoted_message_text(item: &QuotedMessageItem) -> Option<String> {
    let content = item.body.as_ref()?.content.as_str();
    let text = match item.msg_type.as_str() {
        "text" => serde_json::from_str::<super::types::TextContent>(content)
            .ok()
            .map(|text| text.text),
        "post" => extract_post_content(content)
            .map(|(_, text, _)| text)
            .filter(|text| !text.is_empty()),
        "interactive" => extract_interactive_card_text(content),
        "image" => Some("[Quoted image]".to_owned()),
        other => Some(format!("[Quoted {other} message]")),
    }?;
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    Some(text.chars().take(4_000).collect())
}

fn extract_interactive_card_text(content: &str) -> Option<String> {
    let card: serde_json::Value = serde_json::from_str(content).ok()?;
    let mut parts = Vec::new();
    collect_card_text(&card, &mut parts);
    if parts.is_empty() { None } else { Some(parts.join("\n")) }
}

fn collect_card_text(value: &serde_json::Value, parts: &mut Vec<String>) {
    if let Some(text) = value.get("content").and_then(|content| content.as_str()) {
        parts.push(text.to_owned());
    }
    if let Some(elements) = value.get("elements").and_then(|elements| elements.as_array()) {
        for element in elements {
            collect_card_text(element, parts);
        }
    }
}

/// Persist a downloaded Lark attachment under the OS temp directory.
///
/// Returns the absolute path. The file must remain on disk because the agent
/// reads attachments lazily (Aionrs loads images through its ViewImage tool).
/// Download every attachment that carries a Lark resource key and record its
/// local path, so the orchestrator can forward it to the agent.
async fn download_lark_attachments(api: &LarkApi, message_id: &str, attachments: &mut [UnifiedAttachment]) {
    for attachment in attachments.iter_mut() {
        // Quoted attachments are downloaded with the quoted message id and
        // then merged into this message's list; a second attempt here would
        // use the wrong message id (Lark answers 234003 File not in msg).
        if attachment.local_path.is_some() {
            continue;
        }
        let Some(file_key) = attachment.file_id.clone().filter(|key| !key.is_empty()) else {
            continue;
        };
        // Rich-text posts can mix images and files in one message, so the
        // resource kind is per attachment rather than per message.
        let resource_type = if attachment
            .mime_type
            .as_deref()
            .is_some_and(|mime| mime.starts_with("image/"))
        {
            "image"
        } else {
            "file"
        };
        match api
            .download_message_resource(message_id, &file_key, resource_type)
            .await
        {
            Ok(resource) => match save_downloaded_attachment(message_id, &file_key, attachment, &resource).await {
                Ok(path) => {
                    attachment.local_path = Some(path);
                    if attachment.mime_type.is_none() {
                        attachment.mime_type = resource.content_type;
                    }
                }
                Err(err) => {
                    warn!(error = %err, file_key = %file_key, "failed to persist Lark attachment");
                }
            },
            Err(err) => {
                warn!(error = %err, file_key = %file_key, "failed to download Lark attachment");
            }
        }
    }
}

async fn save_downloaded_attachment(
    message_id: &str,
    file_key: &str,
    attachment: &UnifiedAttachment,
    resource: &DownloadedResource,
) -> std::io::Result<String> {
    let dir = std::env::temp_dir().join("aionui").join("lark");
    tokio::fs::create_dir_all(&dir).await?;
    let path = dir.join(format!(
        "lark_{}_{}.{}",
        sanitize_filename_part(message_id),
        sanitize_filename_part(file_key),
        attachment_extension(attachment, resource)
    ));
    tokio::fs::write(&path, &resource.bytes).await?;
    Ok(path.to_string_lossy().into_owned())
}

fn attachment_extension(attachment: &UnifiedAttachment, resource: &DownloadedResource) -> String {
    if let Some(ext) = attachment
        .file_name
        .as_deref()
        .and_then(|name| std::path::Path::new(name).extension().and_then(|ext| ext.to_str()))
        .filter(|ext| !ext.is_empty())
    {
        return ext.to_ascii_lowercase();
    }

    let content_type = resource
        .content_type
        .as_deref()
        .or(attachment.mime_type.as_deref())
        .unwrap_or_default();
    match content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "image/jpeg" | "image/jpg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/bmp" => "bmp",
        "audio/mpeg" | "audio/mp3" => "mp3",
        "audio/ogg" | "audio/opus" => "ogg",
        "audio/wav" | "audio/x-wav" => "wav",
        _ => "bin",
    }
    .to_owned()
}

fn sanitize_filename_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .take(80)
        .collect()
}

// ---------------------------------------------------------------------------
// Event deduplication
// ---------------------------------------------------------------------------

/// Check if an event ID has been seen recently. If not, mark it as seen.
async fn is_duplicate(cache: &Arc<Mutex<HashMap<String, Instant>>>, event_id: &str) -> bool {
    let snapshot = {
        let mut map = cache.lock().await;
        if map.contains_key(event_id) {
            return true;
        }
        map.insert(event_id.to_string(), Instant::now());
        map.clone()
    };
    // Keep the on-disk copy in sync outside the lock. Events are low-volume.
    // Unit tests use synthetic ids and must not touch the user's store.
    #[cfg(not(test))]
    persist_events(&snapshot);
    #[cfg(test)]
    drop(snapshot);
    false
}

/// Returns the message age in seconds when it exceeds `MAX_EVENT_AGE`.
fn stale_event_age(create_time: Option<&str>) -> Option<i64> {
    let created_ms = create_time?.parse::<i64>().ok()?;
    let age_ms = chrono_now().saturating_mul(1000).saturating_sub(created_ms);
    (age_ms > MAX_EVENT_AGE.as_millis() as i64).then_some(age_ms / 1000)
}

fn dedup_store_path() -> std::path::PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("aionui")
        .join("lark")
        .join(LARK_EVENT_DEDUP_FILE)
}

fn load_persisted_events() -> HashMap<String, Instant> {
    let path = dedup_store_path();
    let Ok(raw) = std::fs::read_to_string(&path) else {
        return HashMap::new();
    };
    let Ok(entries) = serde_json::from_str::<HashMap<String, i64>>(&raw) else {
        warn!(path = %path.display(), "ignoring corrupt Lark event dedup store");
        return HashMap::new();
    };
    let now = chrono_now();
    let loaded: HashMap<String, Instant> = entries
        .into_iter()
        .filter_map(|(event_id, stored_at)| {
            let age = Duration::from_secs(now.saturating_sub(stored_at).max(0) as u64);
            if age >= LARK_EVENT_DEDUP_TTL {
                return None;
            }
            // `checked_sub` fails when the entry predates the machine's
            // monotonic clock origin; such an entry is far outside the retry
            // window, so dropping it is safe.
            Instant::now().checked_sub(age).map(|instant| (event_id, instant))
        })
        .collect();
    if !loaded.is_empty() {
        debug!(entries = loaded.len(), "restored Lark event dedup store");
    }
    loaded
}

/// Write recent event ids to disk so retries after a restart are deduplicated.
fn persist_events(map: &HashMap<String, Instant>) {
    let path = dedup_store_path();
    let Some(parent) = path.parent() else {
        return;
    };
    if let Err(e) = std::fs::create_dir_all(parent) {
        warn!(error = %e, "failed to create Lark event dedup directory");
        return;
    }
    let now = chrono_now();
    let entries: HashMap<&str, i64> = map
        .iter()
        .filter(|(_, instant)| instant.elapsed() < LARK_EVENT_DEDUP_TTL)
        .map(|(event_id, instant)| {
            (
                event_id.as_str(),
                now.saturating_sub(instant.elapsed().as_secs() as i64),
            )
        })
        .collect();
    match serde_json::to_string(&entries) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                warn!(error = %e, "failed to persist Lark event dedup store");
            }
        }
        Err(e) => warn!(error = %e, "failed to encode Lark event dedup store"),
    }
}

/// Remove expired entries from the dedup cache.
async fn cleanup_expired_events(cache: &Arc<Mutex<HashMap<String, Instant>>>) {
    let mut map = cache.lock().await;
    let before = map.len();
    map.retain(|_, instant| instant.elapsed() < LARK_EVENT_DEDUP_TTL);
    let removed = before - map.len();
    if removed > 0 {
        debug!(removed, remaining = map.len(), "Lark dedup cache cleanup");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncate a message to the platform limit, appending "..." if truncated.
fn truncate_message(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_string();
    }
    let truncated: String = text.chars().take(limit - 3).collect();
    format!("{truncated}...")
}

/// Calculate exponential backoff delay, capped at the maximum.
fn backoff_delay(attempt: u32) -> Duration {
    let delay_secs = 2u64.saturating_pow(attempt).min(MAX_RECONNECT_DELAY.as_secs());
    Duration::from_secs(delay_secs)
}

/// Current unix timestamp in seconds.
fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Build a TLS connector for WebSocket connections.
///
/// Explicitly sets ALPN to `http/1.1` only — WebSocket requires an HTTP/1.1
/// upgrade handshake and is incompatible with h2. Without this, some servers
/// negotiate h2 via ALPN and the WebSocket upgrade never completes.
fn build_ws_tls_connector() -> Result<tokio_tungstenite::Connector, ChannelError> {
    use std::sync::Arc;
    use tokio_tungstenite::Connector;

    let certs = rustls_native_certs::load_native_certs();
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add_parsable_certificates(certs.certs);

    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));

    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| ChannelError::ConnectionFailed(format!("TLS config error: {e}")))?
        .with_root_certificates(root_store)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    Ok(Connector::Rustls(Arc::new(config)))
}

/// Extract service_id from a WebSocket URL query string.
fn extract_service_id(url: &str) -> i32 {
    url.split('?')
        .nth(1)
        .unwrap_or("")
        .split('&')
        .find_map(|param| {
            let (k, v) = param.split_once('=')?;
            if k == "service_id" { v.parse().ok() } else { None }
        })
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- truncate_message ---------------------------------------------------

    #[test]
    fn truncate_within_limit() {
        assert_eq!(truncate_message("Hello", 100), "Hello");
    }

    #[test]
    fn truncate_at_limit() {
        assert_eq!(truncate_message("abc", 3), "abc");
    }

    #[test]
    fn truncate_exceeds_limit() {
        let result = truncate_message("Hello, world!", 10);
        assert_eq!(result, "Hello, ...");
    }

    #[test]
    fn truncate_unicode() {
        let result = truncate_message("你好世界测试文本", 5);
        assert_eq!(result, "你好...");
    }

    // -- backoff_delay ------------------------------------------------------

    #[test]
    fn backoff_exponential() {
        assert_eq!(backoff_delay(1), Duration::from_secs(2));
        assert_eq!(backoff_delay(2), Duration::from_secs(4));
        assert_eq!(backoff_delay(3), Duration::from_secs(8));
    }

    #[test]
    fn backoff_capped() {
        assert_eq!(backoff_delay(5), Duration::from_secs(30));
        assert_eq!(backoff_delay(10), Duration::from_secs(30));
    }

    // -- extract_message_content --------------------------------------------

    #[test]
    fn extract_text_content() {
        let (ct, text, att) = extract_message_content("text", r#"{"text":"Hello"}"#, None);
        assert_eq!(ct, MessageContentType::Text);
        assert_eq!(text, "Hello");
        assert!(att.is_none());
    }

    #[test]
    fn extract_text_command() {
        let (ct, text, _) = extract_message_content("text", r#"{"text":"/start"}"#, None);
        assert_eq!(ct, MessageContentType::Command);
        assert_eq!(text, "/start");
    }

    #[test]
    fn extract_text_strips_mentions() {
        use super::super::types::{Mention, MentionId};
        let mentions = vec![Mention {
            key: "@_user_1".into(),
            id: MentionId {
                open_id: "ou_bot".into(),
                user_id: String::new(),
                union_id: String::new(),
            },
            name: "Bot".into(),
        }];
        let (ct, text, _) = extract_message_content("text", r#"{"text":"@_user_1 Hello bot"}"#, Some(&mentions));
        assert_eq!(ct, MessageContentType::Text);
        assert_eq!(text, "Hello bot");
    }

    #[test]
    fn extract_image_content() {
        let (ct, _, att) = extract_message_content("image", r#"{"image_key":"img_123"}"#, None);
        assert_eq!(ct, MessageContentType::Photo);
        let atts = att.unwrap();
        assert_eq!(atts[0].file_id.as_deref(), Some("img_123"));
    }

    // -- downloaded attachments ---------------------------------------------

    fn downloaded(bytes: &[u8], content_type: Option<&str>) -> DownloadedResource {
        DownloadedResource {
            bytes: bytes.to_vec(),
            content_type: content_type.map(str::to_owned),
        }
    }

    fn attachment_with_name(name: Option<&str>, mime: Option<&str>) -> UnifiedAttachment {
        UnifiedAttachment {
            file_id: Some("file_key".into()),
            file_name: name.map(str::to_owned),
            mime_type: mime.map(str::to_owned),
            file_size: None,
            url: None,
            local_path: None,
        }
    }

    #[test]
    fn attachment_extension_prefers_original_file_name() {
        let att = attachment_with_name(Some("Report.PDF"), Some("application/pdf"));
        assert_eq!(attachment_extension(&att, &downloaded(b"x", Some("image/png"))), "pdf");
    }

    #[test]
    fn attachment_extension_falls_back_to_content_type() {
        let att = attachment_with_name(None, None);
        assert_eq!(attachment_extension(&att, &downloaded(b"x", Some("image/png"))), "png");
        assert_eq!(attachment_extension(&att, &downloaded(b"x", Some("image/jpeg"))), "jpg");
        assert_eq!(attachment_extension(&att, &downloaded(b"x", None)), "bin");
    }

    #[test]
    fn sanitize_filename_part_keeps_safe_characters_only() {
        assert_eq!(sanitize_filename_part("om_123/../img"), "om_123____img");
    }

    #[tokio::test]
    async fn save_downloaded_attachment_writes_file_with_mime_extension() {
        let att = attachment_with_name(None, None);
        let path = save_downloaded_attachment("om_123", "img_456", &att, &downloaded(b"jpeg-bytes", Some("image/png")))
            .await
            .unwrap();
        assert!(path.ends_with(".png"), "unexpected path: {path}");
        assert_eq!(std::fs::read(&path).unwrap(), b"jpeg-bytes");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn extract_file_content() {
        let (ct, _, att) = extract_message_content("file", r#"{"file_key":"file_123","file_name":"doc.pdf"}"#, None);
        assert_eq!(ct, MessageContentType::Document);
        let atts = att.unwrap();
        assert_eq!(atts[0].file_id.as_deref(), Some("file_123"));
        assert_eq!(atts[0].file_name.as_deref(), Some("doc.pdf"));
    }

    #[test]
    fn extract_audio_content() {
        let (ct, _, att) = extract_message_content("audio", r#"{"file_key":"audio_123"}"#, None);
        assert_eq!(ct, MessageContentType::Audio);
        let atts = att.unwrap();
        assert_eq!(atts[0].file_id.as_deref(), Some("audio_123"));
    }

    #[test]
    fn extract_post_content_with_text_and_image() {
        let raw = r#"{"title":"Report","content":[[{"tag":"text","text":"Look at this: "},{"tag":"img","image_key":"img_1"}],[{"tag":"a","text":"docs","href":"https://example.com"}]]}"#;
        let (ct, text, att) = extract_message_content("post", raw, None);
        assert_eq!(ct, MessageContentType::Text);
        assert!(text.contains("Report"));
        assert!(text.contains("Look at this:"));
        assert!(text.contains("docs (https://example.com)"));
        let atts = att.unwrap();
        assert_eq!(atts.len(), 1);
        assert_eq!(atts[0].file_id.as_deref(), Some("img_1"));
        assert_eq!(atts[0].mime_type.as_deref(), Some("image/jpeg"));
    }

    #[test]
    fn extract_post_content_localized() {
        let raw = r#"{"zh_cn":{"title":"标题","content":[[{"tag":"text","text":"你好"}]]}}"#;
        let (ct, text, att) = extract_message_content("post", raw, None);
        assert_eq!(ct, MessageContentType::Text);
        assert!(text.contains("标题"));
        assert!(text.contains("你好"));
        assert!(att.is_none());
    }

    #[test]
    fn extract_post_content_renders_at_code_and_media() {
        let raw = r#"{"content":[[{"tag":"at","user_id":"ou_1","user_name":"Alice"},{"tag":"code_block","language":"rust","text":"fn main() {}"}],[{"tag":"media","file_key":"file_1","file_name":"report.pdf"}]]}"#;
        let (_, text, att) = extract_message_content("post", raw, None);
        assert!(text.contains("@Alice"));
        assert!(text.contains("```rust"));
        let atts = att.unwrap();
        assert_eq!(atts[0].file_id.as_deref(), Some("file_1"));
        assert_eq!(atts[0].file_name.as_deref(), Some("report.pdf"));
    }

    #[test]
    fn extract_post_content_invalid_falls_back() {
        let (ct, text, att) = extract_message_content("post", "not-json", None);
        assert_eq!(ct, MessageContentType::Text);
        assert!(text.contains("Unsupported rich text"));
        assert!(att.is_none());
    }

    #[test]
    fn quoted_text_message_is_extracted() {
        let item: QuotedMessageItem = serde_json::from_value(serde_json::json!({
            "message_id": "om_parent",
            "msg_type": "text",
            "body": { "content": "{\"text\":\"quoted hello\"}" }
        }))
        .unwrap();
        assert_eq!(quoted_message_text(&item).as_deref(), Some("quoted hello"));
    }

    #[test]
    fn quoted_interactive_card_extracts_markdown_without_buttons() {
        let card = serde_json::json!({
            "elements": [
                { "tag": "markdown", "content": "bot reply" },
                { "tag": "action", "actions": [
                    { "tag": "button", "text": { "tag": "plain_text", "content": "New Session" } }
                ]}
            ]
        })
        .to_string();
        let item: QuotedMessageItem = serde_json::from_value(serde_json::json!({
            "message_id": "om_parent",
            "msg_type": "interactive",
            "body": { "content": card }
        }))
        .unwrap();

        let text = quoted_message_text(&item).unwrap();
        assert!(text.contains("bot reply"));
        assert!(!text.contains("New Session"));
    }

    #[test]
    fn quoted_post_message_is_extracted() {
        let post = serde_json::json!({
            "title": "Title",
            "content": [[{ "tag": "text", "text": "post body" }]]
        })
        .to_string();
        let item: QuotedMessageItem = serde_json::from_value(serde_json::json!({
            "message_id": "om_parent",
            "msg_type": "post",
            "body": { "content": post }
        }))
        .unwrap();

        let text = quoted_message_text(&item).unwrap();
        assert!(text.contains("Title"));
        assert!(text.contains("post body"));
    }

    #[test]
    fn extract_unsupported_type() {
        let (ct, text, _) = extract_message_content("sticker", "{}", None);
        assert_eq!(ct, MessageContentType::Text);
        assert!(text.contains("Unsupported"));
    }

    // -- is_duplicate -------------------------------------------------------

    #[tokio::test]
    async fn dedup_first_seen_not_duplicate() {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        assert!(!is_duplicate(&cache, "ev_1").await);
    }

    #[tokio::test]
    async fn dedup_second_seen_is_duplicate() {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        is_duplicate(&cache, "ev_1").await;
        assert!(is_duplicate(&cache, "ev_1").await);
    }

    #[tokio::test]
    async fn dedup_different_ids_not_duplicate() {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        is_duplicate(&cache, "ev_1").await;
        assert!(!is_duplicate(&cache, "ev_2").await);
    }

    // -- cleanup_expired_events ---------------------------------------------

    #[tokio::test]
    async fn cleanup_removes_expired_entries() {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut map = cache.lock().await;
            // Insert an entry that is already expired
            map.insert(
                "old".into(),
                Instant::now() - LARK_EVENT_DEDUP_TTL - Duration::from_secs(1),
            );
            map.insert("recent".into(), Instant::now());
        }
        cleanup_expired_events(&cache).await;
        let map = cache.lock().await;
        assert!(!map.contains_key("old"));
        assert!(map.contains_key("recent"));
    }

    // -- extract_service_id ---------------------------------------------------

    #[test]
    fn extract_service_id_from_url() {
        let url = "wss://open.feishu.cn/ws/abc?service_id=7&device_id=xxx";
        assert_eq!(extract_service_id(url), 7);
    }

    #[test]
    fn extract_service_id_missing_returns_zero() {
        let url = "wss://open.feishu.cn/ws/abc";
        assert_eq!(extract_service_id(url), 0);
    }

    // -- handle_control_frame -------------------------------------------------

    #[test]
    fn control_frame_pong_with_config_returns_duration() {
        use super::super::frame::PbFrame;
        let frame = PbFrame {
            seq_id: 0,
            log_id: 0,
            service: 1,
            method: 0,
            headers: vec![super::super::frame::PbHeader {
                key: "type".into(),
                value: "pong".into(),
            }],
            payload_encoding: String::new(),
            payload_type: String::new(),
            payload: br#"{"PingInterval":60,"ReconnectCount":5,"ReconnectInterval":30,"ReconnectNonce":10}"#.to_vec(),
            log_id_new: String::new(),
        };
        let result = handle_control_frame(&frame);
        assert_eq!(result, Some(Duration::from_secs(60)));
    }

    #[test]
    fn control_frame_pong_empty_payload_returns_none() {
        use super::super::frame::PbFrame;
        let frame = PbFrame {
            seq_id: 0,
            log_id: 0,
            service: 1,
            method: 0,
            headers: vec![super::super::frame::PbHeader {
                key: "type".into(),
                value: "pong".into(),
            }],
            payload_encoding: String::new(),
            payload_type: String::new(),
            payload: Vec::new(),
            log_id_new: String::new(),
        };
        let result = handle_control_frame(&frame);
        assert_eq!(result, None);
    }

    #[test]
    fn control_frame_ping_returns_none() {
        use super::super::frame::PbFrame;
        let frame = PbFrame {
            seq_id: 0,
            log_id: 0,
            service: 1,
            method: 0,
            headers: vec![super::super::frame::PbHeader {
                key: "type".into(),
                value: "ping".into(),
            }],
            payload_encoding: String::new(),
            payload_type: String::new(),
            payload: Vec::new(),
            log_id_new: String::new(),
        };
        let result = handle_control_frame(&frame);
        assert_eq!(result, None);
    }

    // -- handle_ws_text integration -------------------------------------------

    #[test]
    fn stale_event_age_detects_old_messages() {
        let now = chrono_now();
        let fresh = ((now - 60) * 1000).to_string();
        assert_eq!(stale_event_age(Some(&fresh)), None);

        let old = ((now - MAX_EVENT_AGE.as_secs() as i64 - 1) * 1000).to_string();
        assert!(stale_event_age(Some(&old)).is_some());

        assert_eq!(stale_event_age(None), None);
        assert_eq!(stale_event_age(Some("not-a-number")), None);
    }

    #[test]
    fn card_action_payload_detection() {
        let event = r#"{"header":{"event_type":"card.action.trigger"}}"#;
        assert!(is_card_action_payload(event, "event"));
        assert!(is_card_action_payload("{}", "card"));

        let message = r#"{"header":{"event_type":"im.message.receive_v1"}}"#;
        assert!(!is_card_action_payload(message, "event"));
        assert!(!is_card_action_payload("not json", "event"));
        assert!(!is_card_action_payload("{}", "unknown"));
    }

    #[tokio::test]
    async fn ws_text_event_dispatches_message() {
        let (message_tx, mut message_rx) = mpsc::channel(16);
        let (confirm_tx, _confirm_rx) = mpsc::channel(16);
        let dedup_cache = Arc::new(Mutex::new(HashMap::new()));

        let payload = r#"{
            "header": {
                "event_id": "ev_test_1",
                "event_type": "im.message.receive_v1"
            },
            "event": {
                "sender": {
                    "sender_id": { "open_id": "ou_user1", "user_id": "", "union_id": "" },
                    "sender_type": "user",
                    "tenant_key": "t1"
                },
                "message": {
                    "message_id": "msg_test_1",
                    "chat_id": "oc_chat1",
                    "chat_type": "p2p",
                    "message_type": "text",
                    "content": "{\"text\":\"Hello bot\"}"
                }
            }
        }"#;

        handle_ws_text(payload, "event", &message_tx, &confirm_tx, &dedup_cache, None).await;

        let msg = message_rx.try_recv().unwrap();
        assert_eq!(msg.id, "msg_test_1");
        assert_eq!(msg.chat_id, "oc_chat1");
        assert_eq!(msg.content.text, "Hello bot");
        assert_eq!(msg.user.id, "ou_user1");
        assert_eq!(msg.platform, PluginType::Lark);
    }

    #[tokio::test]
    async fn ws_text_event_deduplicates() {
        let (message_tx, mut message_rx) = mpsc::channel(16);
        let (confirm_tx, _confirm_rx) = mpsc::channel(16);
        let dedup_cache = Arc::new(Mutex::new(HashMap::new()));

        let payload = r#"{
            "header": {
                "event_id": "ev_dup_1",
                "event_type": "im.message.receive_v1"
            },
            "event": {
                "sender": {
                    "sender_id": { "open_id": "ou_x", "user_id": "", "union_id": "" },
                    "sender_type": "user",
                    "tenant_key": "t1"
                },
                "message": {
                    "message_id": "msg_dup",
                    "chat_id": "oc_1",
                    "chat_type": "p2p",
                    "message_type": "text",
                    "content": "{\"text\":\"hi\"}"
                }
            }
        }"#;

        handle_ws_text(payload, "event", &message_tx, &confirm_tx, &dedup_cache, None).await;
        handle_ws_text(payload, "event", &message_tx, &confirm_tx, &dedup_cache, None).await;

        assert!(message_rx.try_recv().is_ok());
        assert!(message_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn ws_text_card_dispatches_action() {
        let (message_tx, mut message_rx) = mpsc::channel(16);
        let (confirm_tx, _confirm_rx) = mpsc::channel(16);
        let dedup_cache = Arc::new(Mutex::new(HashMap::new()));

        let payload = r#"{
            "operator": { "open_id": "ou_actor", "user_id": null },
            "action": { "tag": "button", "value": {"action": "chat:help.show"} },
            "open_message_id": "om_card1",
            "open_chat_id": "oc_chat2"
        }"#;

        handle_ws_text(payload, "card", &message_tx, &confirm_tx, &dedup_cache, None).await;

        let msg = message_rx.try_recv().unwrap();
        assert_eq!(msg.chat_id, "oc_chat2");
        assert_eq!(msg.user.id, "ou_actor");
        assert!(msg.action.is_some());
        let action = msg.action.unwrap();
        assert_eq!(action.action, "help.show");
    }

    #[tokio::test]
    async fn ws_text_card_action_event_dispatches_and_returns_response() {
        let (message_tx, mut message_rx) = mpsc::channel(16);
        let (confirm_tx, _confirm_rx) = mpsc::channel(16);
        let dedup_cache = Arc::new(Mutex::new(HashMap::new()));

        let payload = r#"{
            "header": { "event_id": "evt_card_1", "event_type": "card.action.trigger" },
            "event": {
                "operator": { "open_id": "ou_actor", "user_id": null },
                "action": { "tag": "button", "value": { "action": "system:session.new" } },
                "token": "tok",
                "context": {
                    "open_message_id": "om_card1",
                    "open_chat_id": "oc_chat2"
                }
            }
        }"#;

        let response = handle_ws_text(payload, "event", &message_tx, &confirm_tx, &dedup_cache, None).await;
        let response = response.expect("card action must return a callback response");
        assert!(response.contains("toast"));

        let msg = message_rx.try_recv().unwrap();
        assert_eq!(msg.chat_id, "oc_chat2");
        assert_eq!(msg.user.id, "ou_actor");
        let action = msg.action.unwrap();
        assert_eq!(action.action, "session.new");
    }

    #[tokio::test]
    async fn ws_text_card_confirm_sends_to_confirm_channel() {
        let (message_tx, _message_rx) = mpsc::channel(16);
        let (confirm_tx, mut confirm_rx) = mpsc::channel(16);
        let dedup_cache = Arc::new(Mutex::new(HashMap::new()));

        // NOTE: handle_card_action checks `action == "system.confirm"` but
        // parse_lark_callback splits "system:confirm:..." into category="system",
        // action="confirm". This means the confirm path requires the action field
        // to literally contain "system.confirm" as a dotted string within the
        // "system" category. The format "system:system.confirm:k=v" satisfies this.
        let payload = r#"{
            "operator": { "open_id": "ou_actor" },
            "action": { "tag": "button", "value": {"action": "system:system.confirm:callId=call_123,value=approve"} },
            "open_message_id": "om_1",
            "open_chat_id": "oc_1"
        }"#;

        handle_ws_text(payload, "card", &message_tx, &confirm_tx, &dedup_cache, None).await;

        let (call_id, value) = confirm_rx.try_recv().unwrap();
        assert_eq!(call_id, "call_123");
        assert_eq!(value, "approve");
    }

    #[tokio::test]
    async fn ws_text_unknown_type_does_not_dispatch() {
        let (message_tx, mut message_rx) = mpsc::channel(16);
        let (confirm_tx, _confirm_rx) = mpsc::channel(16);
        let dedup_cache = Arc::new(Mutex::new(HashMap::new()));

        handle_ws_text("{}", "unknown_type", &message_tx, &confirm_tx, &dedup_cache, None).await;

        assert!(message_rx.try_recv().is_err());
    }

    // -- LarkPlugin constructor ---------------------------------------------

    #[test]
    fn new_plugin_initial_state() {
        let plugin = LarkPlugin::new();
        assert_eq!(plugin.status(), PluginStatus::Created);
        assert!(plugin.bot_info().is_none());
        assert!(plugin.last_error().is_none());
        assert_eq!(plugin.plugin_type(), PluginType::Lark);
        assert_eq!(plugin.active_user_count(), 0);
    }
}
