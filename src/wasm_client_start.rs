use super::*;

const MAX_DEGRADED_DIAGNOSTICS: usize = 128;
// Credential callbacks cross a privileged host boundary. Keep one proof small
// and short-lived so a stalled enrollment cannot retain an unbounded secret.
const MAX_ENROLLMENT_CREDENTIAL_BYTES: usize = 16 * 1024;
const MAX_ENROLLMENT_CREDENTIAL_LIFETIME_MS: f64 = 5.0 * 60.0 * 1_000.0;

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
                    record_failure(
                        &mut failures,
                        format_js_error("could not create discovery request", error),
                    );
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
                            record_failure(
                                &mut failures,
                                format!(
                                    "host discovery returned more than configured maxPeers {}",
                                    config.max_peers
                                ),
                            );
                        }
                    }
                    Err(error) => record_failure(
                        &mut failures,
                        format_js_error("host discovery failed", error),
                    ),
                }
            }
            Ok(_) => record_failure(
                &mut failures,
                "host discovery failed: callback must return an array of peer endpoints"
                    .to_string(),
            ),
            Err(error) => record_failure(
                &mut failures,
                format_js_error("host discovery failed", error),
            ),
        }
    }
    if is_cancelled(&state) {
        return StartOutcome::Cancelled;
    }
    let (peers, truncated) = deduplicate_peers(peers, config.max_peers);
    if truncated {
        record_failure(
            &mut failures,
            format!(
                "peer discovery exceeded configured maxPeers {}",
                config.max_peers
            ),
        );
    }
    let discovered_peer_count = peers.len();
    let mut authenticated_peers = Vec::new();
    let mut authenticated_node_ids = BTreeSet::new();
    for peer in peers {
        if is_cancelled(&state) {
            return StartOutcome::Cancelled;
        }
        let Some(enroll) = &host.enroll else {
            record_failure(
                &mut failures,
                format!(
                    "host enrollment capability is required for {}:{}",
                    peer.host, peer.port
                ),
            );
            continue;
        };
        let credential =
            match issue_enrollment_credential(host, &config.identity, &peer, state.clone()).await {
                Ok(credential) => credential,
                Err(CredentialIssueError::SignerUnavailable(error)) => {
                    record_failure(
                        &mut failures,
                        format_js_error(
                            &format!(
                                "enrollment signer unavailable for {}:{}",
                                peer.host, peer.port
                            ),
                            error,
                        ),
                    );
                    continue;
                }
                Err(CredentialIssueError::Expired) => {
                    record_failure(
                        &mut failures,
                        format!(
                            "enrollment credential expired for {}:{}",
                            peer.host, peer.port
                        ),
                    );
                    continue;
                }
                Err(CredentialIssueError::Invalid(error)) => {
                    record_failure(
                        &mut failures,
                        format_js_error(
                            &format!(
                                "enrollment credential invalid for {}:{}",
                                peer.host, peer.port
                            ),
                            error,
                        ),
                    );
                    continue;
                }
            };
        let request = match enrollment_request_to_js(
            &config.identity,
            &peer,
            credential.as_ref(),
            state.clone(),
        ) {
            Ok(request) => request,
            Err(error) => {
                record_failure(
                    &mut failures,
                    format_js_error("could not create enrollment request", error),
                );
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
                Err(error) => record_failure(
                    &mut failures,
                    format_js_error("peer authentication failed", error),
                ),
            },
            Err(error) => record_failure(
                &mut failures,
                format_js_error("peer enrollment failed", error),
            ),
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

enum CredentialIssueError {
    SignerUnavailable(JsValue),
    Expired,
    Invalid(JsValue),
}

async fn issue_enrollment_credential(
    host: &HostCapabilities,
    identity: &NodeIdentity,
    peer: &PeerEndpoint,
    state: Rc<RefCell<ClientState>>,
) -> Result<Option<JsValue>, CredentialIssueError> {
    let Some(issuer) = &host.issue_enrollment_credential else {
        return Ok(None);
    };
    let request = enrollment_credential_request_to_js(identity, peer, state)
        .map_err(CredentialIssueError::Invalid)?;
    let credential = await_callback(issuer, request)
        .await
        .map_err(CredentialIssueError::SignerUnavailable)?;
    parse_enrollment_credential(&credential).map(Some)
}

fn enrollment_credential_request_to_js(
    identity: &NodeIdentity,
    peer: &PeerEndpoint,
    state: Rc<RefCell<ClientState>>,
) -> Result<JsValue, JsValue> {
    let request = Object::new();
    set_string(&request, "purpose", ENROLLMENT_CREDENTIAL_PURPOSE)?;
    Reflect::set(
        &request,
        &JsValue::from_str("identity"),
        &identity_to_js(identity)?,
    )?;
    Reflect::set(
        &request,
        &JsValue::from_str("candidate"),
        &peer_to_js(peer)?,
    )?;
    Reflect::set(
        &request,
        &JsValue::from_str("isCancelled"),
        &cancellation_callback(state),
    )?;
    Ok(request.into())
}

fn parse_enrollment_credential(value: &JsValue) -> Result<JsValue, CredentialIssueError> {
    let purpose = required_string(value, "purpose").map_err(CredentialIssueError::Invalid)?;
    if purpose != ENROLLMENT_CREDENTIAL_PURPOSE {
        return Err(CredentialIssueError::Invalid(JsValue::from_str(
            "credential purpose must be manyfold.peer-enrollment.v1",
        )));
    }
    let token = required_string(value, "token").map_err(CredentialIssueError::Invalid)?;
    if token.len() > MAX_ENROLLMENT_CREDENTIAL_BYTES {
        return Err(CredentialIssueError::Invalid(JsValue::from_str(
            "credential token cannot exceed 16384 bytes",
        )));
    }
    let expires_at_unix_ms = Reflect::get(value, &JsValue::from_str("expiresAtUnixMs"))
        .map_err(CredentialIssueError::Invalid)?
        .as_f64()
        .filter(|expiry| expiry.is_finite())
        .ok_or_else(|| {
            CredentialIssueError::Invalid(JsValue::from_str(
                "credential expiresAtUnixMs must be a finite number",
            ))
        })?;
    let now = js_sys::Date::now();
    if expires_at_unix_ms <= now {
        return Err(CredentialIssueError::Expired);
    }
    if expires_at_unix_ms > now + MAX_ENROLLMENT_CREDENTIAL_LIFETIME_MS {
        return Err(CredentialIssueError::Invalid(JsValue::from_str(
            "credential expiresAtUnixMs must be within five minutes",
        )));
    }
    let credential = Object::new();
    set_string(&credential, "purpose", ENROLLMENT_CREDENTIAL_PURPOSE)
        .map_err(CredentialIssueError::Invalid)?;
    set_string(&credential, "token", &token).map_err(CredentialIssueError::Invalid)?;
    Reflect::set(
        &credential,
        &JsValue::from_str("expiresAtUnixMs"),
        &JsValue::from_f64(expires_at_unix_ms),
    )
    .map_err(CredentialIssueError::Invalid)?;
    Ok(credential.into())
}

fn record_failure(failures: &mut Vec<String>, failure: String) {
    if failures.len() < MAX_DEGRADED_DIAGNOSTICS {
        failures.push(failure);
    }
}
