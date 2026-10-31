use aionui_ai_agent::AgentStreamEvent;
use aionui_api_types::ChatFileRef;
use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::error::ChannelError;

/// Result of handing a channel message to a team.
pub struct TeamDispatchResult {
    /// Live event stream for the leader's conversation. The orchestrator feeds
    /// this into the same stream relay used by single-assistant conversations.
    ///
    /// `None` when the leader runtime was not available for subscription; the
    /// orchestrator then only acknowledges that the message was delivered.
    pub stream_rx: Option<broadcast::Receiver<AgentStreamEvent>>,
}

/// Port for dispatching a channel message to a team.
///
/// `aionui-channel` must not depend on `aionui-team` (domain layering), so the
/// composition layer (`aionui-app`) implements this over `TeamSessionService`.
#[async_trait]
pub trait ChannelTeamDispatcher: Send + Sync {
    /// Deliver `text` (and downloaded files) to the team's leader and return a
    /// subscription to the leader's reply stream.
    async fn dispatch_to_team(
        &self,
        owner_user_id: &str,
        team_id: &str,
        text: &str,
        files: Vec<ChatFileRef>,
    ) -> Result<TeamDispatchResult, ChannelError>;
}
