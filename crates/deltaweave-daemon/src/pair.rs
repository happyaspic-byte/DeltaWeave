//! Pairing tickets issued from the live daemon server address.

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use deltaweave_daemon_api::CommandResult;
use deltaweave_net::{
    Identity, NetworkMode, PeerPolicy, Server, ServerConfig, access, load_or_create_identity,
    redeem_pairing_ticket, start_server,
};

const DEFAULT_TTL_SECONDS: u64 = 600;

/// Inputs for a pairing-capable daemon endpoint.
#[derive(Clone, Debug)]
pub struct PairingConfig {
    /// Private access/index/store directory.
    pub state_root: PathBuf,
    /// Folder the server would receive into.
    pub destination_root: PathBuf,
    /// Persistent node key path.
    pub identity_path: PathBuf,
    /// Optional fixed UDP bind.
    pub bind: Option<std::net::SocketAddr>,
}

/// Owns one iroh server plus its access store for ticket issue/redeem/revoke.
pub struct PairingService {
    identity: Identity,
    access: Arc<access::AccessStore>,
    server: Option<Server>,
}

impl PairingService {
    /// Binds an iroh server and waits until a direct address is advertised.
    pub async fn start(config: PairingConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.state_root).with_context(|| {
            format!(
                "failed to create pairing state root {}",
                config.state_root.display()
            )
        })?;
        std::fs::create_dir_all(&config.destination_root).with_context(|| {
            format!(
                "failed to create pairing destination {}",
                config.destination_root.display()
            )
        })?;
        let identity = load_or_create_identity(&config.identity_path)?;
        let access = Arc::new(access::AccessStore::open(
            config.state_root.join("access.redb"),
        )?);
        let server = start_server(ServerConfig {
            secret_key: identity.secret_key.clone(),
            destination_root: config.destination_root,
            state_root: config.state_root,
            peer_policy: PeerPolicy::Durable(Arc::clone(&access)),
            network_mode: NetworkMode::DirectOnly,
            quota_policy: None,
            bind: config.bind,
        })
        .await?;
        let _ = server.wait_online(Duration::from_secs(5)).await;
        Ok(Self {
            identity,
            access,
            server: Some(server),
        })
    }

    /// Issues a single-use ticket bound to the live direct address.
    pub fn issue_ticket(&self, ttl_seconds: Option<u64>) -> Result<CommandResult> {
        let server = self
            .server
            .as_ref()
            .context("pairing server is not running")?;
        let direct = server
            .address_info()
            .direct_addresses
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("server has no direct address"))?;
        let ttl = ttl_seconds.unwrap_or(DEFAULT_TTL_SECONDS);
        ensure!(ttl > 0, "ttl_seconds must be greater than zero");
        let expires_at = access::unix_now()
            .checked_add(ttl)
            .context("pairing ticket expiry overflow")?;
        let ticket = self
            .access
            .issue_ticket(&self.identity.endpoint_id(), &direct, expires_at)?;
        let server_endpoint_id = ticket.server_endpoint_id.clone();
        Ok(CommandResult::TicketIssued {
            code: ticket.to_code(),
            expires_at: ticket.expires_at,
            server_fingerprint: fingerprint(&server_endpoint_id),
            server_endpoint_id,
        })
    }

    /// Redeems a printable ticket using this daemon's identity.
    pub async fn redeem_ticket(&self, code: &str) -> Result<CommandResult> {
        let ticket = access::PairingTicket::from_code(code)?;
        let peer_endpoint_id = ticket.server_endpoint_id.clone();
        let outcome = redeem_pairing_ticket(
            self.identity.secret_key.clone(),
            ticket,
            NetworkMode::DirectOnly,
        )
        .await?;
        let outcome = match outcome {
            access::RedeemOutcome::Paired => "paired",
            access::RedeemOutcome::AlreadyPaired => "already_paired",
        };
        Ok(CommandResult::TicketRedeemed {
            outcome: outcome.into(),
            peer_fingerprint: fingerprint(&peer_endpoint_id),
            peer_endpoint_id,
        })
    }

    /// Revokes a previously authorized peer.
    pub fn revoke_peer(&self, endpoint_id: &str) -> Result<bool> {
        let peer = endpoint_id.parse().context("invalid endpoint ID")?;
        self.access.revoke(peer)
    }

    /// Shuts down the iroh server.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(server) = self.server.take() {
            server.shutdown().await?;
        }
        Ok(())
    }
}

fn fingerprint(endpoint_id: &str) -> String {
    endpoint_id.chars().take(16).collect()
}
