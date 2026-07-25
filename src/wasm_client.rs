use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use js_sys::{Array, Function, Object, Promise, Reflect};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{future_to_promise, JsFuture};

use crate::wasm_api::{CallbackPlacement, PubSub, WorkerProxy};

#[path = "wasm_client_start.rs"]
mod start;

use start::{run_start, ClientCallback, ClientCallbackState, ClientState, StartOutcome};

const DEFAULT_MAX_PEERS: usize = 128;
const DEFAULT_RETAINED_MESSAGES: usize = 1024;
const MAX_STATUS_CALLBACKS: usize = 128;
const ENROLLMENT_CREDENTIAL_PURPOSE: &str = "manyfold.peer-enrollment.v1";

thread_local! {
    static NEXT_INSTANCE_ID: Cell<u64> = const { Cell::new(1) };
}

#[wasm_bindgen(typescript_custom_section)]
const TYPESCRIPT_CLIENT_TYPES: &str = include_str!("wasm_client_types.d.ts");

#[wasm_bindgen]
#[derive(Clone)]
pub struct NodeIdentity {
    cluster_id: String,
    node_id: String,
    instance_id: String,
}

#[wasm_bindgen]
#[derive(Clone)]
pub struct PeerEndpoint {
    host: String,
    port: u16,
    server_name: Option<String>,
}

#[wasm_bindgen]
#[derive(Clone)]
pub struct ClientConfig {
    identity: NodeIdentity,
    callback_placement: CallbackPlacement,
    static_peers: Vec<PeerEndpoint>,
    retained_messages: usize,
    max_peers: usize,
}

#[wasm_bindgen]
#[derive(Clone)]
pub struct HostCapabilities {
    kind: String,
    discover: Option<Function>,
    issue_enrollment_credential: Option<Function>,
    enroll: Option<Function>,
    thread_scheduler: Option<Function>,
    native_worker_spawner: Option<Function>,
    shutdown: Option<Function>,
}

#[wasm_bindgen]
#[derive(Clone)]
pub struct ClientStatus {
    state: String,
    detail: String,
    discovered_peer_count: usize,
    authenticated_peer_count: usize,
    failure_count: usize,
    failures: Vec<String>,
}

#[wasm_bindgen]
pub struct ClientStatusSubscription {
    id: u64,
    callbacks: Rc<RefCell<ClientCallbackState>>,
    is_disposed: bool,
}

#[wasm_bindgen]
pub struct ManyfoldClient {
    config: ClientConfig,
    state: Rc<RefCell<ClientState>>,
    callbacks: Rc<RefCell<ClientCallbackState>>,
    host: Rc<RefCell<Option<HostCapabilities>>>,
    proxy: WorkerProxy,
}

#[wasm_bindgen]
impl NodeIdentity {
    #[wasm_bindgen(constructor)]
    pub fn new(
        cluster_id: String,
        node_id: String,
        instance_id: Option<String>,
    ) -> Result<NodeIdentity, JsValue> {
        Ok(Self {
            cluster_id: require_text(cluster_id, "clusterId")?,
            node_id: require_text(node_id, "nodeId")?,
            instance_id: match instance_id {
                Some(value) => require_text(value, "instanceId")?,
                None => generated_instance_id(),
            },
        })
    }

    #[wasm_bindgen(getter, js_name = clusterId)]
    pub fn cluster_id(&self) -> String {
        self.cluster_id.clone()
    }

    #[wasm_bindgen(getter, js_name = nodeId)]
    pub fn node_id(&self) -> String {
        self.node_id.clone()
    }

    #[wasm_bindgen(getter, js_name = instanceId)]
    pub fn instance_id(&self) -> String {
        self.instance_id.clone()
    }
}

#[wasm_bindgen]
impl PeerEndpoint {
    #[wasm_bindgen(constructor)]
    pub fn new(
        host: String,
        port: u32,
        server_name: Option<String>,
    ) -> Result<PeerEndpoint, JsValue> {
        Ok(Self {
            host: require_text(host, "peer host")?,
            port: require_port(port)?,
            server_name: server_name
                .map(|value| require_text(value, "serverName"))
                .transpose()?,
        })
    }

    #[wasm_bindgen(getter)]
    pub fn host(&self) -> String {
        self.host.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn port(&self) -> u16 {
        self.port
    }

    #[wasm_bindgen(getter, js_name = serverName)]
    pub fn server_name(&self) -> Option<String> {
        self.server_name.clone()
    }
}

#[wasm_bindgen]
impl ClientConfig {
    #[wasm_bindgen(constructor)]
    pub fn new(
        identity: &NodeIdentity,
        callback_placement: Option<CallbackPlacement>,
        static_peers: Option<Array>,
        retained_messages: Option<usize>,
        max_peers: Option<usize>,
    ) -> Result<ClientConfig, JsValue> {
        let max_peers = require_positive(max_peers.unwrap_or(DEFAULT_MAX_PEERS), "maxPeers")?;
        let (static_peers, truncated) =
            parse_peer_array(static_peers.unwrap_or_default(), max_peers)?;
        if truncated {
            return Err(JsValue::from_str(
                "staticPeers cannot contain more entries than maxPeers",
            ));
        }
        Ok(Self {
            identity: identity.clone(),
            callback_placement: callback_placement.unwrap_or_else(CallbackPlacement::inline),
            static_peers: deduplicate_peers(static_peers, max_peers).0,
            retained_messages: require_positive(
                retained_messages.unwrap_or(DEFAULT_RETAINED_MESSAGES),
                "retainedMessages",
            )?,
            max_peers,
        })
    }

    #[wasm_bindgen(getter)]
    pub fn identity(&self) -> NodeIdentity {
        self.identity.clone()
    }

    #[wasm_bindgen(getter, js_name = callbackPlacement)]
    pub fn callback_placement(&self) -> CallbackPlacement {
        self.callback_placement.clone()
    }

    #[wasm_bindgen(getter, js_name = staticPeers)]
    pub fn static_peers(&self) -> Array {
        peer_array_to_js(&self.static_peers)
    }

    #[wasm_bindgen(getter, js_name = retainedMessages)]
    pub fn retained_messages(&self) -> usize {
        self.retained_messages
    }

    #[wasm_bindgen(getter, js_name = maxPeers)]
    pub fn max_peers(&self) -> usize {
        self.max_peers
    }
}

#[wasm_bindgen]
impl HostCapabilities {
    #[wasm_bindgen(constructor)]
    pub fn new(kind: String) -> Result<HostCapabilities, JsValue> {
        let kind = require_text(kind, "host kind")?;
        if !matches!(kind.as_str(), "browser" | "electron" | "desktop") {
            return Err(JsValue::from_str(
                "host kind must be browser, electron, or desktop",
            ));
        }
        Ok(Self {
            kind,
            discover: None,
            issue_enrollment_credential: None,
            enroll: None,
            thread_scheduler: None,
            native_worker_spawner: None,
            shutdown: None,
        })
    }

    #[wasm_bindgen(getter)]
    pub fn kind(&self) -> String {
        self.kind.clone()
    }

    #[wasm_bindgen(js_name = setDiscovery)]
    pub fn set_discovery(&mut self, callback: &Function) {
        self.discover = Some(callback.clone());
    }

    #[wasm_bindgen(js_name = setEnrollmentCredentialIssuer)]
    pub fn set_enrollment_credential_issuer(&mut self, callback: &Function) {
        self.issue_enrollment_credential = Some(callback.clone());
    }

    #[wasm_bindgen(js_name = setEnrollment)]
    pub fn set_enrollment(&mut self, callback: &Function) {
        self.enroll = Some(callback.clone());
    }

    #[wasm_bindgen(js_name = setThreadScheduler)]
    pub fn set_thread_scheduler(&mut self, callback: &Function) {
        self.thread_scheduler = Some(callback.clone());
    }

    #[wasm_bindgen(js_name = setNativeWorkerSpawner)]
    pub fn set_native_worker_spawner(&mut self, callback: &Function) -> Result<(), JsValue> {
        if self.kind == "browser" {
            return Err(JsValue::from_str(
                "browser hosts cannot provide native worker spawning",
            ));
        }
        self.native_worker_spawner = Some(callback.clone());
        Ok(())
    }

    #[wasm_bindgen(js_name = setShutdown)]
    pub fn set_shutdown(&mut self, callback: &Function) {
        self.shutdown = Some(callback.clone());
    }

    #[wasm_bindgen(getter, js_name = hasDiscovery)]
    pub fn has_discovery(&self) -> bool {
        self.discover.is_some()
    }

    #[wasm_bindgen(getter, js_name = hasEnrollmentCredentialIssuer)]
    pub fn has_enrollment_credential_issuer(&self) -> bool {
        self.issue_enrollment_credential.is_some()
    }

    #[wasm_bindgen(getter, js_name = hasEnrollment)]
    pub fn has_enrollment(&self) -> bool {
        self.enroll.is_some()
    }

    #[wasm_bindgen(getter, js_name = hasThreadScheduler)]
    pub fn has_thread_scheduler(&self) -> bool {
        self.thread_scheduler.is_some()
    }

    #[wasm_bindgen(getter, js_name = canSpawnNativeWorkers)]
    pub fn can_spawn_native_workers(&self) -> bool {
        self.native_worker_spawner.is_some()
    }
}

#[wasm_bindgen]
impl ClientStatus {
    #[wasm_bindgen(getter)]
    pub fn state(&self) -> String {
        self.state.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn detail(&self) -> String {
        self.detail.clone()
    }

    #[wasm_bindgen(getter, js_name = discoveredPeerCount)]
    pub fn discovered_peer_count(&self) -> usize {
        self.discovered_peer_count
    }

    #[wasm_bindgen(getter, js_name = authenticatedPeerCount)]
    pub fn authenticated_peer_count(&self) -> usize {
        self.authenticated_peer_count
    }

    #[wasm_bindgen(getter, js_name = failureCount)]
    pub fn failure_count(&self) -> usize {
        self.failure_count
    }

    #[wasm_bindgen(getter)]
    pub fn failures(&self) -> Array {
        self.failures
            .iter()
            .map(|failure| JsValue::from_str(failure))
            .collect()
    }

    #[wasm_bindgen(getter, js_name = isReady)]
    pub fn is_ready(&self) -> bool {
        self.state == "ready"
    }

    #[wasm_bindgen(getter, js_name = isDegraded)]
    pub fn is_degraded(&self) -> bool {
        self.state == "degraded"
    }
}

#[wasm_bindgen]
impl ClientStatusSubscription {
    #[wasm_bindgen(getter, js_name = isDisposed)]
    pub fn is_disposed(&self) -> bool {
        self.is_disposed
    }

    pub fn dispose(&mut self) -> bool {
        if self.is_disposed {
            return false;
        }
        let removed = self
            .callbacks
            .borrow_mut()
            .callbacks
            .remove(&self.id)
            .is_some();
        self.is_disposed = true;
        removed
    }
}

#[wasm_bindgen]
impl ManyfoldClient {
    #[wasm_bindgen(constructor)]
    pub fn new(config: &ClientConfig, host: &HostCapabilities) -> Result<ManyfoldClient, JsValue> {
        let runtime_id = format!("{}:{}", config.identity.cluster_id, config.identity.node_id);
        let proxy = WorkerProxy::with_runtime(runtime_id, config.retained_messages)?;
        proxy.set_desktop_spawner(host.native_worker_spawner.as_ref());
        proxy.set_thread_scheduler(host.thread_scheduler.as_ref());
        Ok(Self {
            config: config.clone(),
            state: Rc::new(RefCell::new(ClientState::default())),
            callbacks: Rc::new(RefCell::new(ClientCallbackState::default())),
            host: Rc::new(RefCell::new(Some(host.clone()))),
            proxy,
        })
    }

    #[wasm_bindgen(getter)]
    pub fn identity(&self) -> NodeIdentity {
        self.config.identity.clone()
    }

    #[wasm_bindgen(getter, js_name = hostKind)]
    pub fn host_kind(&self) -> Result<String, JsValue> {
        self.host
            .borrow()
            .as_ref()
            .map(|host| host.kind.clone())
            .ok_or_else(|| JsValue::from_str("Manyfold client is shut down"))
    }

    #[wasm_bindgen(getter, js_name = isDisposed)]
    pub fn is_disposed(&self) -> bool {
        self.state.borrow().is_disposed
    }

    pub fn status(&self) -> ClientStatus {
        self.state.borrow().status()
    }

    #[wasm_bindgen(js_name = authenticatedPeers)]
    pub fn authenticated_peers(&self) -> Array {
        let peers = Array::new();
        for peer in &self.state.borrow().authenticated_peers {
            peers.push(&JsValue::from(peer.clone()));
        }
        peers
    }

    #[wasm_bindgen(js_name = onStatus)]
    pub fn on_status(
        &self,
        callback: &Function,
        placement: Option<CallbackPlacement>,
    ) -> Result<ClientStatusSubscription, JsValue> {
        self.require_not_disposed()?;
        let id = {
            let mut callbacks = self.callbacks.borrow_mut();
            if callbacks.callbacks.len() == MAX_STATUS_CALLBACKS {
                return Err(JsValue::from_str(
                    "ManyfoldClient status callback limit reached",
                ));
            }
            let id = callbacks.next_id;
            callbacks.next_id += 1;
            callbacks.callbacks.insert(
                id,
                ClientCallback {
                    callback: callback.clone(),
                    placement: placement.unwrap_or_else(|| self.config.callback_placement.clone()),
                    pending: 0,
                },
            );
            id
        };
        if let Err(error) = deliver_client_status(
            self.callbacks.clone(),
            id,
            self.status(),
            self.thread_scheduler(),
        ) {
            self.callbacks.borrow_mut().callbacks.remove(&id);
            return Err(error);
        }
        Ok(ClientStatusSubscription {
            id,
            callbacks: self.callbacks.clone(),
            is_disposed: false,
        })
    }

    pub fn pubsub(&self, topic: String) -> Result<PubSub, JsValue> {
        self.require_not_disposed()?;
        Ok(self.proxy.pubsub(require_text(topic, "topic")?))
    }

    pub fn start(&self) -> Result<Promise, JsValue> {
        {
            let mut state = self.state.borrow_mut();
            if state.is_disposed {
                return Err(JsValue::from_str("Manyfold client is shut down"));
            }
            if state.start_in_progress || matches!(state.phase.as_str(), "ready" | "degraded") {
                return Err(JsValue::from_str("Manyfold client is already started"));
            }
            state.start_in_progress = true;
            state.cancel_requested = false;
            state.phase = "starting".to_string();
            state.detail = "discovering peer endpoints".to_string();
            state.discovered_peer_count = 0;
            state.authenticated_peers.clear();
            state.failures.clear();
        }
        if let Err(error) = self.emit_status() {
            let mut state = self.state.borrow_mut();
            state.start_in_progress = false;
            state.phase = "stopped".to_string();
            state.detail = "status callback rejected startup".to_string();
            return Err(error);
        }

        let config = self.config.clone();
        let state = self.state.clone();
        let callbacks = self.callbacks.clone();
        let host = self
            .host
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| JsValue::from_str("Manyfold client is shut down"))?;
        let thread_scheduler = host.thread_scheduler.clone();
        Ok(future_to_promise(async move {
            let outcome = run_start(&config, &host, state.clone()).await;
            let status = {
                let mut state = state.borrow_mut();
                if state.is_disposed {
                    state.start_in_progress = false;
                    state.status()
                } else {
                    match outcome {
                        StartOutcome::Cancelled => {
                            state.phase = "stopped".to_string();
                            state.detail = "start cancelled".to_string();
                            state.discovered_peer_count = 0;
                            state.authenticated_peers.clear();
                            state.failures.clear();
                        }
                        StartOutcome::Complete {
                            discovered_peer_count,
                            authenticated_peers,
                            failures,
                        } => {
                            state.discovered_peer_count = discovered_peer_count;
                            state.authenticated_peers = authenticated_peers;
                            state.failures = failures;
                            if state.failures.is_empty() {
                                state.phase = "ready".to_string();
                                state.detail = if state.authenticated_peers.is_empty() {
                                    "local runtime ready".to_string()
                                } else {
                                    "runtime ready with authenticated peers".to_string()
                                };
                            } else {
                                state.phase = "degraded".to_string();
                                state.detail =
                                    "runtime started with discovery or enrollment failures"
                                        .to_string();
                            }
                        }
                    }
                    state.start_in_progress = false;
                    state.status()
                }
            };
            deliver_all_client_statuses(callbacks, status.clone(), thread_scheduler)?;
            Ok(JsValue::from(status))
        }))
    }

    #[wasm_bindgen(js_name = cancelStart)]
    pub fn cancel_start(&self) -> bool {
        let mut state = self.state.borrow_mut();
        if !state.start_in_progress || state.is_disposed {
            return false;
        }
        state.cancel_requested = true;
        true
    }

    pub fn shutdown(&self) -> Result<Promise, JsValue> {
        let host = {
            let mut state = self.state.borrow_mut();
            if state.is_disposed {
                return Ok(Promise::resolve(&JsValue::FALSE));
            }
            state.is_disposed = true;
            state.cancel_requested = true;
            state.phase = "shutting_down".to_string();
            state.detail = "releasing host and runtime resources".to_string();
            self.host.borrow().clone()
        };
        self.emit_status()?;
        self.proxy.close()?;

        let state = self.state.clone();
        let callbacks = self.callbacks.clone();
        let host_owner = self.host.clone();
        let identity = self.config.identity.clone();
        Ok(future_to_promise(async move {
            let shutdown_error = match host.as_ref().and_then(|host| host.shutdown.as_ref()) {
                Some(callback) => {
                    let request = shutdown_request_to_js(
                        &identity,
                        state.borrow().authenticated_peers.len(),
                    )?;
                    await_callback(callback, request).await.err()
                }
                None => None,
            };
            let status = {
                let mut state = state.borrow_mut();
                state.start_in_progress = false;
                state.phase = "stopped".to_string();
                state.detail = match shutdown_error {
                    Some(error) => {
                        state.failures = vec![format_js_error("host shutdown failed", error)];
                        "client shut down after a host cleanup failure".to_string()
                    }
                    None => {
                        state.failures.clear();
                        "client shut down".to_string()
                    }
                };
                state.discovered_peer_count = 0;
                state.authenticated_peers.clear();
                state.status()
            };
            let thread_scheduler = host.as_ref().and_then(|host| host.thread_scheduler.clone());
            deliver_all_client_statuses(callbacks.clone(), status, thread_scheduler)?;
            callbacks.borrow_mut().callbacks.clear();
            *host_owner.borrow_mut() = None;
            Ok(JsValue::TRUE)
        }))
    }
}

impl ManyfoldClient {
    fn require_not_disposed(&self) -> Result<(), JsValue> {
        if self.state.borrow().is_disposed {
            return Err(JsValue::from_str("Manyfold client is shut down"));
        }
        Ok(())
    }

    fn thread_scheduler(&self) -> Option<Function> {
        self.host
            .borrow()
            .as_ref()
            .and_then(|host| host.thread_scheduler.clone())
    }

    fn emit_status(&self) -> Result<(), JsValue> {
        deliver_all_client_statuses(
            self.callbacks.clone(),
            self.status(),
            self.thread_scheduler(),
        )
    }
}

async fn await_callback(callback: &Function, request: JsValue) -> Result<JsValue, JsValue> {
    let value = callback.call1(&JsValue::UNDEFINED, &request)?;
    JsFuture::from(Promise::resolve(&value)).await
}

fn discovery_request_to_js(
    identity: &NodeIdentity,
    static_peers: &[PeerEndpoint],
    state: Rc<RefCell<ClientState>>,
) -> Result<JsValue, JsValue> {
    let request = Object::new();
    Reflect::set(
        &request,
        &JsValue::from_str("identity"),
        &identity_to_js(identity)?,
    )?;
    Reflect::set(
        &request,
        &JsValue::from_str("staticPeers"),
        &peer_array_to_js(static_peers),
    )?;
    Reflect::set(
        &request,
        &JsValue::from_str("isCancelled"),
        &cancellation_callback(state),
    )?;
    Ok(request.into())
}

fn enrollment_request_to_js(
    identity: &NodeIdentity,
    peer: &PeerEndpoint,
    credential: Option<&JsValue>,
    state: Rc<RefCell<ClientState>>,
) -> Result<JsValue, JsValue> {
    let request = Object::new();
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
    if let Some(credential) = credential {
        Reflect::set(&request, &JsValue::from_str("credential"), credential)?;
    }
    Reflect::set(
        &request,
        &JsValue::from_str("isCancelled"),
        &cancellation_callback(state),
    )?;
    Ok(request.into())
}

fn shutdown_request_to_js(
    identity: &NodeIdentity,
    authenticated_peer_count: usize,
) -> Result<JsValue, JsValue> {
    let request = Object::new();
    Reflect::set(
        &request,
        &JsValue::from_str("identity"),
        &identity_to_js(identity)?,
    )?;
    Reflect::set(
        &request,
        &JsValue::from_str("authenticatedPeerCount"),
        &JsValue::from_f64(authenticated_peer_count as f64),
    )?;
    Ok(request.into())
}

fn cancellation_callback(state: Rc<RefCell<ClientState>>) -> JsValue {
    Closure::wrap(Box::new(move || is_cancelled(&state)) as Box<dyn Fn() -> bool>).into_js_value()
}

fn parse_enrollment_result(
    value: &JsValue,
    local_identity: &NodeIdentity,
) -> Result<NodeIdentity, JsValue> {
    if !required_bool(value, "authenticated")? {
        let detail = optional_string(value, "error")?
            .unwrap_or_else(|| "host rejected peer credentials".to_string());
        return Err(JsValue::from_str(&detail));
    }
    let identity = Reflect::get(value, &JsValue::from_str("identity"))?;
    if identity.is_null() || identity.is_undefined() {
        return Err(JsValue::from_str(
            "authenticated enrollment result requires identity",
        ));
    }
    let peer = identity_from_js(&identity)?;
    if peer.cluster_id != local_identity.cluster_id {
        return Err(JsValue::from_str(
            "authenticated peer belongs to another cluster",
        ));
    }
    if peer.node_id == local_identity.node_id {
        return Err(JsValue::from_str(
            "authenticated peer cannot claim the local node id",
        ));
    }
    Ok(peer)
}

fn deliver_all_client_statuses(
    callbacks: Rc<RefCell<ClientCallbackState>>,
    status: ClientStatus,
    thread_scheduler: Option<Function>,
) -> Result<(), JsValue> {
    let ids = callbacks
        .borrow()
        .callbacks
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for id in ids {
        deliver_client_status(
            callbacks.clone(),
            id,
            status.clone(),
            thread_scheduler.clone(),
        )?;
    }
    Ok(())
}

fn deliver_client_status(
    callbacks: Rc<RefCell<ClientCallbackState>>,
    id: u64,
    status: ClientStatus,
    thread_scheduler: Option<Function>,
) -> Result<(), JsValue> {
    let (callback, placement) = {
        let mut callbacks = callbacks.borrow_mut();
        let Some(callback) = callbacks.callbacks.get_mut(&id) else {
            return Ok(());
        };
        if callback.pending >= callback.placement.queue_limit() {
            return Err(JsValue::from_str(
                "ManyfoldClient status callback queue is full",
            ));
        }
        callback.pending += 1;
        (callback.callback.clone(), callback.placement.clone())
    };
    let action_callbacks = callbacks.clone();
    let action = Closure::once_into_js(move || {
        let result = callback.call1(&JsValue::UNDEFINED, &JsValue::from(status));
        clear_pending_client_callback(action_callbacks, id);
        if let Err(error) = result {
            wasm_bindgen::throw_val(error);
        }
    });
    match placement.kind().as_str() {
        "inline" => action
            .dyn_into::<Function>()?
            .call0(&JsValue::UNDEFINED)
            .map(|_| ()),
        "main" => schedule_main(action, callbacks, id),
        "thread" => schedule_thread(
            thread_scheduler.ok_or_else(|| {
                JsValue::from_str(
                    "thread callback placement requires a host-provided thread scheduler",
                )
            })?,
            placement
                .thread_name()
                .as_deref()
                .expect("thread placement validates a thread name"),
            action,
            callbacks,
            id,
        ),
        _ => Err(JsValue::from_str(
            "callback placement kind must be inline, main, or thread",
        )),
    }
}

fn schedule_main(
    action: JsValue,
    callbacks: Rc<RefCell<ClientCallbackState>>,
    id: u64,
) -> Result<(), JsValue> {
    let global = js_sys::global();
    let scheduler = Reflect::get(&global, &JsValue::from_str("queueMicrotask"))?;
    if !scheduler.is_function() {
        return action
            .dyn_into::<Function>()?
            .call0(&JsValue::UNDEFINED)
            .map(|_| ());
    }
    match scheduler.dyn_into::<Function>()?.call1(&global, &action) {
        Ok(_) => Ok(()),
        Err(error) => {
            clear_pending_client_callback(callbacks, id);
            Err(error)
        }
    }
}

fn schedule_thread(
    scheduler: Function,
    thread_name: &str,
    action: JsValue,
    callbacks: Rc<RefCell<ClientCallbackState>>,
    id: u64,
) -> Result<(), JsValue> {
    match scheduler.call2(
        &JsValue::UNDEFINED,
        &JsValue::from_str(thread_name),
        &action,
    ) {
        Ok(_) => Ok(()),
        Err(error) => {
            clear_pending_client_callback(callbacks, id);
            Err(error)
        }
    }
}

fn clear_pending_client_callback(callbacks: Rc<RefCell<ClientCallbackState>>, id: u64) {
    if let Some(callback) = callbacks.borrow_mut().callbacks.get_mut(&id) {
        callback.pending = callback.pending.saturating_sub(1);
    }
}

fn parse_peer_array(values: Array, limit: usize) -> Result<(Vec<PeerEndpoint>, bool), JsValue> {
    let truncated = values.length() as usize > limit;
    let peers = values
        .iter()
        .take(limit)
        .map(|value| endpoint_from_js(&value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((peers, truncated))
}

fn deduplicate_peers(peers: Vec<PeerEndpoint>, max_peers: usize) -> (Vec<PeerEndpoint>, bool) {
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    let mut truncated = false;
    for peer in peers {
        let key = (peer.host.clone(), peer.port, peer.server_name.clone());
        if !seen.insert(key) {
            continue;
        }
        if result.len() == max_peers {
            truncated = true;
            continue;
        }
        result.push(peer);
    }
    (result, truncated)
}

fn peer_array_to_js(peers: &[PeerEndpoint]) -> Array {
    let values = Array::new();
    for peer in peers {
        values.push(&peer_to_js(peer).expect("validated peer endpoint"));
    }
    values
}

fn peer_to_js(peer: &PeerEndpoint) -> Result<JsValue, JsValue> {
    let value = Object::new();
    set_string(&value, "host", &peer.host)?;
    Reflect::set(
        &value,
        &JsValue::from_str("port"),
        &JsValue::from_f64(peer.port as f64),
    )?;
    if let Some(server_name) = &peer.server_name {
        set_string(&value, "serverName", server_name)?;
    }
    Ok(value.into())
}

fn endpoint_from_js(value: &JsValue) -> Result<PeerEndpoint, JsValue> {
    let host = required_string(value, "host")?;
    let port = Reflect::get(value, &JsValue::from_str("port"))?
        .as_f64()
        .ok_or_else(|| JsValue::from_str("peer port must be a number"))?;
    if port.fract() != 0.0 || !(1.0..=65535.0).contains(&port) {
        return Err(JsValue::from_str(
            "peer port must be an integer between 1 and 65535",
        ));
    }
    PeerEndpoint::new(host, port as u32, optional_string(value, "serverName")?)
}

fn identity_to_js(identity: &NodeIdentity) -> Result<JsValue, JsValue> {
    let value = Object::new();
    set_string(&value, "clusterId", &identity.cluster_id)?;
    set_string(&value, "nodeId", &identity.node_id)?;
    set_string(&value, "instanceId", &identity.instance_id)?;
    Ok(value.into())
}

fn identity_from_js(value: &JsValue) -> Result<NodeIdentity, JsValue> {
    NodeIdentity::new(
        required_string(value, "clusterId")?,
        required_string(value, "nodeId")?,
        Some(required_string(value, "instanceId")?),
    )
}

fn required_string(value: &JsValue, name: &str) -> Result<String, JsValue> {
    Reflect::get(value, &JsValue::from_str(name))?
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("{name} must be a string")))
        .and_then(|value| require_text(value, name))
}

fn optional_string(value: &JsValue, name: &str) -> Result<Option<String>, JsValue> {
    let value = Reflect::get(value, &JsValue::from_str(name))?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    value
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("{name} must be a string")))
        .map(Some)
}

fn required_bool(value: &JsValue, name: &str) -> Result<bool, JsValue> {
    Reflect::get(value, &JsValue::from_str(name))?
        .as_bool()
        .ok_or_else(|| JsValue::from_str(&format!("{name} must be a boolean")))
}

fn set_string(target: &Object, name: &str, value: &str) -> Result<(), JsValue> {
    Reflect::set(target, &JsValue::from_str(name), &JsValue::from_str(value))?;
    Ok(())
}

fn require_text(value: String, name: &str) -> Result<String, JsValue> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(JsValue::from_str(&format!(
            "{name} must be a non-empty string"
        )));
    }
    Ok(trimmed.to_string())
}

fn require_port(port: u32) -> Result<u16, JsValue> {
    if !(1..=65535).contains(&port) {
        return Err(JsValue::from_str("peer port must be between 1 and 65535"));
    }
    Ok(port as u16)
}

fn require_positive(value: usize, name: &str) -> Result<usize, JsValue> {
    if value == 0 {
        return Err(JsValue::from_str(&format!("{name} must be positive")));
    }
    Ok(value)
}

fn generated_instance_id() -> String {
    let sequence = NEXT_INSTANCE_ID.with(|next| {
        let sequence = next.get();
        next.set(sequence + 1);
        sequence
    });
    format!("wasm-{}-{sequence}", js_sys::Date::now() as u64)
}

fn is_cancelled(state: &Rc<RefCell<ClientState>>) -> bool {
    let state = state.borrow();
    state.cancel_requested || state.is_disposed
}

fn format_js_error(context: &str, error: JsValue) -> String {
    let detail = error
        .as_string()
        .or_else(|| {
            Reflect::get(&error, &JsValue::from_str("message"))
                .ok()
                .and_then(|value| value.as_string())
        })
        .unwrap_or_else(|| "unknown host error".to_string());
    format!("{context}: {detail}")
}
