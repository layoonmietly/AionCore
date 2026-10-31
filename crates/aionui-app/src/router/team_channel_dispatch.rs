//! Channel → team dispatch adapter.
//!
//! `aionui-channel` cannot depend on `aionui-team` (domain layering), so the
//! channel owns a `ChannelTeamDispatcher` port and this adapter implements it
//! over `TeamSessionService` + the worker task manager.
//!
//! A team run spans multiple leader turns (the leader delegates, teammates
//! work, then the leader is woken with their results). The adapter therefore
//! forwards the leader's stream for the whole run and only emits the terminal
//! `Finish` frame when the team run itself completes.

use std::sync::Arc;
use std::time::Duration;

use aionui_ai_agent::protocol::events::FinishEventData;
use aionui_ai_agent::{AgentStreamEvent, IWorkerTaskManager};
use aionui_api_types::{ChatFileRef, WebSocketMessage};
use aionui_channel::error::ChannelError;
use aionui_channel::team_dispatch::{ChannelTeamDispatcher, TeamDispatchResult};
use aionui_realtime::BroadcastEventBus;
use aionui_team::{TeamError, TeamSessionService};
use tokio::sync::broadcast;
use tracing::warn;

/// How long to wait for the leader's runtime to attach after enqueueing.
const LEADER_TASK_SUBSCRIBE_ATTEMPTS: usize = 50;
const LEADER_TASK_SUBSCRIBE_INTERVAL: Duration = Duration::from_millis(100);

/// Hard cap for one team run relay (a stuck run must not hold a card forever).
const TEAM_RUN_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Give trailing leader frames a moment to land after the run is marked done.
const TEAM_RUN_FINISH_GRACE: Duration = Duration::from_secs(2);

const TERMINAL_TEAM_EVENT_NAMES: [&str; 3] = ["team.runCompleted", "team.runFailed", "team.runCancelled"];

pub(crate) struct TeamChannelDispatcher {
    team: Arc<TeamSessionService>,
    task_manager: Arc<dyn IWorkerTaskManager>,
    event_bus: Arc<BroadcastEventBus>,
}

impl TeamChannelDispatcher {
    pub fn new(
        team: Arc<TeamSessionService>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        event_bus: Arc<BroadcastEventBus>,
    ) -> Self {
        Self {
            team,
            task_manager,
            event_bus,
        }
    }
}

#[async_trait::async_trait]
impl ChannelTeamDispatcher for TeamChannelDispatcher {
    async fn dispatch_to_team(
        &self,
        owner_user_id: &str,
        team_id: &str,
        text: &str,
        files: Vec<ChatFileRef>,
    ) -> Result<TeamDispatchResult, ChannelError> {
        // Subscribe before enqueueing so a fast run terminal event is not lost.
        let team_events = self.event_bus.subscribe();

        self.team
            .ensure_session(owner_user_id, team_id)
            .await
            .map_err(map_team_error)?;

        let team = self
            .team
            .get_team(owner_user_id, team_id)
            .await
            .map_err(map_team_error)?;
        let leader = team
            .assistants
            .iter()
            .find(|agent| {
                agent.role.eq_ignore_ascii_case("lead")
                    || agent.role.eq_ignore_ascii_case("leader")
                    || team.leader_assistant_id.as_deref() == Some(agent.slot_id.as_str())
            })
            .ok_or_else(|| ChannelError::InvalidConfig("team has no lead agent".to_owned()))?;
        let conversation_id = leader.conversation_id.clone();

        // Subscribe before sending when the leader runtime is already attached,
        // so early stream frames are not missed. On the first message the task
        // is created during enqueue, so fall back to a short retry after send.
        let mut stream_rx = self
            .task_manager
            .get_task(&conversation_id)
            .map(|task| task.subscribe());

        let files = (!files.is_empty()).then_some(files);
        self.team
            .send_message(owner_user_id, team_id, text, files)
            .await
            .map_err(map_team_error)?;

        if stream_rx.is_none() {
            for _ in 0..LEADER_TASK_SUBSCRIBE_ATTEMPTS {
                if let Some(task) = self.task_manager.get_task(&conversation_id) {
                    stream_rx = Some(task.subscribe());
                    break;
                }
                tokio::time::sleep(LEADER_TASK_SUBSCRIBE_INTERVAL).await;
            }
        }

        let Some(agent_rx) = stream_rx else {
            return Ok(TeamDispatchResult { stream_rx: None });
        };

        // Bridge the leader stream to the run's terminal event so the channel
        // relay keeps one card open until the team has actually finished.
        let (tx, rx) = broadcast::channel::<AgentStreamEvent>(256);
        let team_id = team_id.to_owned();
        tokio::spawn(async move {
            relay_team_run(agent_rx, team_events, tx, team_id).await;
        });

        Ok(TeamDispatchResult { stream_rx: Some(rx) })
    }
}

async fn relay_team_run(
    mut agent_rx: broadcast::Receiver<AgentStreamEvent>,
    mut team_events: broadcast::Receiver<WebSocketMessage<serde_json::Value>>,
    tx: broadcast::Sender<AgentStreamEvent>,
    team_id: String,
) {
    let deadline = tokio::time::Instant::now() + TEAM_RUN_TIMEOUT;
    loop {
        tokio::select! {
            event = agent_rx.recv() => match event {
                Ok(AgentStreamEvent::Finish(_)) => {}
                Ok(event) => {
                    let _ = tx.send(event);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(lagged = n, team_id = %team_id, "team leader relay lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    let _ = tx.send(finish_event());
                    break;
                }
            },
            event = team_events.recv() => match event {
                Ok(event) if is_terminal_team_run_event(&event, &team_id) => {
                    // Drain trailing leader frames before closing the card.
                    drain_leader_frames(&mut agent_rx, &tx).await;
                    let _ = tx.send(finish_event());
                    break;
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    let _ = tx.send(finish_event());
                    break;
                }
            },
            _ = tokio::time::sleep_until(deadline) => {
                warn!(team_id = %team_id, "team run relay timed out");
                let _ = tx.send(finish_event());
                break;
            }
        }
    }
}

async fn drain_leader_frames(
    agent_rx: &mut broadcast::Receiver<AgentStreamEvent>,
    tx: &broadcast::Sender<AgentStreamEvent>,
) {
    let deadline = tokio::time::Instant::now() + TEAM_RUN_FINISH_GRACE;
    loop {
        tokio::select! {
            event = agent_rx.recv() => match event {
                Ok(AgentStreamEvent::Finish(_)) => {}
                Ok(event) => {
                    let _ = tx.send(event);
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => {}
            },
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
}

fn is_terminal_team_run_event(event: &WebSocketMessage<serde_json::Value>, team_id: &str) -> bool {
    if !TERMINAL_TEAM_EVENT_NAMES.contains(&event.name.as_str()) {
        return false;
    }
    event.data.get("team_id").and_then(|value| value.as_str()) == Some(team_id)
}

fn finish_event() -> AgentStreamEvent {
    AgentStreamEvent::Finish(FinishEventData { session_id: None })
}

fn map_team_error(error: TeamError) -> ChannelError {
    ChannelError::PlatformApi(format!("team dispatch failed: {error}"))
}
