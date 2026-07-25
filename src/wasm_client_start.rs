use super::*;

pub(super) struct ClientState {
    pub(super) phase: String,
    pub(super) detail: String,
    pub(super) start_in_progress: bool,
    pub(super) cancel_requested: bool,
    pub(super) is_disposed: bool,
    pub(super) discovered_peer_count: usize,
    pub(super) authenticated_peers: Vec<NodeIdentity>,
    pub(super) failures: Vec<String>,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            phase: "stopped".to_string(),
            detail: "client has not started".to_string(),
            start_in_progress: false,
            cancel_requested: false,
            is_disposed: false,
            discovered_peer_count: 0,
            authenticated_peers: Vec::new(),
            failures: Vec::new(),
        }
    }
}

impl ClientState {
    pub(super) fn status(&self) -> ClientStatus {
        ClientStatus {
            state: self.phase.clone(),
            detail: self.detail.clone(),
            discovered_peer_count: self.discovered_peer_count,
            authenticated_peer_count: self.authenticated_peers.len(),
            failure_count: self.failures.len(),
            failures: self.failures.clone(),
        }
    }
}

#[derive(Default)]
pub(super) struct ClientCallbackState {
    pub(super) callbacks: BTreeMap<u64, ClientCallback>,
    pub(super) next_id: u64,
}

pub(super) struct ClientCallback {
    pub(super) callback: Function,
    pub(super) placement: CallbackPlacement,
    pub(super) pending: usize,
}

pub(super) enum StartOutcome {
    Cancelled,
    Complete {
        discovered_peer_count: usize,
        authenticated_peers: Vec<NodeIdentity>,
        failures: Vec<String>,
    },
}

pub(super) async fn run_start(
    config: &ClientConfig,
    host: &HostCapabilities,
    state: Rc<RefCell<ClientState>>,
) -> StartOutcome {
    let mut peers = config.static_peers.clone();
    let mut failures = Vec::new();
    if let Some(discover) = &host.discover {
        let request =
            match discovery_request_to_js(&config.identity, &config.static_peers, state.clone()) {
                Ok(request) => request,
                Err(error) => {
                    failures.push(format_js_error("could not create discovery request", error));
                    return StartOutcome::Complete {
                        discovered_peer_count: peers.len(),
                        authenticated_peers: Vec::new(),
                        failures,
                    };
                }
            };
        match await_callback(discover, request).await {
            Ok(value) if Array::is_array(&value) => {
                match parse_peer_array(Array::from(&value), config.max_peers) {
                    Ok((discovered, was_truncated)) => {
                        peers.extend(discovered);
                        if was_truncated {
                            failures.push(format!(
                                "host discovery returned more than configured maxPeers {}",
                                config.max_peers
                            ));
                        }
                    }
                    Err(error) => failures.push(format_js_error("host discovery failed", error)),
                }
            }
            Ok(_) => failures.push(
                "host discovery failed: callback must return an array of peer endpoints"
                    .to_string(),
            ),
            Err(error) => failures.push(format_js_error("host discovery failed", error)),
        }
    }
    if is_cancelled(&state) {
        return StartOutcome::Cancelled;
    }
    let (peers, truncated) = deduplicate_peers(peers, config.max_peers);
    if truncated {
        failures.push(format!(
            "peer discovery exceeded configured maxPeers {}",
            config.max_peers
        ));
    }
    let discovered_peer_count = peers.len();
    let mut authenticated_peers = Vec::new();
    let mut authenticated_node_ids = BTreeSet::new();
    for peer in peers {
        if is_cancelled(&state) {
            return StartOutcome::Cancelled;
        }
        let Some(enroll) = &host.enroll else {
            failures.push(format!(
                "host enrollment capability is required for {}:{}",
                peer.host, peer.port
            ));
            continue;
        };
        let request = match enrollment_request_to_js(&config.identity, &peer, state.clone()) {
            Ok(request) => request,
            Err(error) => {
                failures.push(format_js_error(
                    "could not create enrollment request",
                    error,
                ));
                continue;
            }
        };
        match await_callback(enroll, request).await {
            Ok(value) => match parse_enrollment_result(&value, &config.identity) {
                Ok(identity) => {
                    if authenticated_node_ids.insert(identity.node_id.clone()) {
                        authenticated_peers.push(identity);
                    }
                }
                Err(error) => failures.push(format_js_error("peer authentication failed", error)),
            },
            Err(error) => failures.push(format_js_error("peer enrollment failed", error)),
        }
    }
    if is_cancelled(&state) {
        return StartOutcome::Cancelled;
    }
    StartOutcome::Complete {
        discovered_peer_count,
        authenticated_peers,
        failures,
    }
}
