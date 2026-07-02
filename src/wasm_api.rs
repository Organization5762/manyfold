use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;

use crate::architecture::{
    WorkerEndpointKind, WorkerEndpointRefCore, WorkerOperation, WorkerProxyCore, WorkerRequestCore,
    WorkerResponseCore,
};

const DEFAULT_RETAINED_MESSAGES: usize = 1024;
const DEFAULT_CALLBACK_QUEUE_LIMIT: usize = 2048;

#[derive(Clone)]
struct WorkerProxy {
    state: Rc<RefCell<WorkerProxyState>>,
}

impl WorkerProxy {
    fn new(retained_messages: usize) -> Result<Self, JsValue> {
        Self::with_runtime("wasm-runtime".to_string(), retained_messages)
    }

    fn with_runtime(runtime_id: String, retained_messages: usize) -> Result<Self, JsValue> {
        Ok(Self {
            state: Rc::new(RefCell::new(WorkerProxyState {
                proxy: WorkerProxyCore::pubsub(runtime_id.clone(), retained_messages)
                    .map_err(js_error)?,
                raw_endpoint: WorkerEndpointRefCore {
                    runtime_id,
                    endpoint_id: "pubsub.raw".to_string(),
                    kind: WorkerEndpointKind::RawData,
                },
                desktop_spawner: None,
                next_request_id: 1,
            })),
        })
    }

    fn pubsub(&self, topic: String) -> PubSub {
        PubSub {
            topic,
            proxy: self.clone(),
            callbacks: Rc::new(RefCell::new(PubSubCallbackState::default())),
        }
    }

    fn lock(&self, name: String) -> Lock {
        Lock {
            name,
            proxy: self.clone(),
        }
    }

    fn clock(&self, name: String) -> Clock {
        Clock {
            name,
            proxy: self.clone(),
        }
    }

    fn set_desktop_spawner(&self, spawner: &Function) {
        self.state.borrow_mut().desktop_spawner = Some(spawner.clone());
    }

    fn clear_desktop_spawner(&self) {
        self.state.borrow_mut().desktop_spawner = None;
    }

    fn spawn_rust_worker(&self, command: String, args: Array) -> Result<JsValue, JsValue> {
        if command.trim().is_empty() {
            return Err(JsValue::from_str(
                "worker command must be a non-empty string",
            ));
        }
        let state = self.state.borrow();
        let spawner = state.desktop_spawner.as_ref().ok_or_else(|| {
            JsValue::from_str("desktop worker spawning requires a host-provided desktop spawner")
        })?;
        let request = Object::new();
        set_string(&request, "command", &command)?;
        set_string(&request, "runtimeId", &state.raw_endpoint.runtime_id)?;
        set_string(
            &request,
            "retainedMessages",
            &state.proxy.retained_messages().to_string(),
        )?;
        Reflect::set(&request, &JsValue::from_str("args"), args.as_ref())?;
        spawner.call1(&JsValue::UNDEFINED, request.as_ref())
    }

    fn message_count(&self) -> usize {
        self.state.borrow().proxy.message_count()
    }

    fn subscriber_count(&self) -> usize {
        self.state.borrow().proxy.subscriber_count()
    }
}

#[wasm_bindgen]
pub struct PubSub {
    topic: String,
    proxy: WorkerProxy,
    callbacks: Rc<RefCell<PubSubCallbackState>>,
}

#[wasm_bindgen]
#[derive(Clone)]
pub struct CallbackPlacement {
    kind: String,
    thread_name: Option<String>,
    queue_limit: usize,
}

#[wasm_bindgen]
pub struct PubSubCallbackSubscription {
    id: u64,
    subscription_name: String,
    callbacks: Rc<RefCell<PubSubCallbackState>>,
    is_disposed: bool,
    proxy: WorkerProxy,
}

#[wasm_bindgen]
impl CallbackPlacement {
    #[wasm_bindgen(constructor)]
    pub fn new(
        kind: Option<String>,
        thread_name: Option<String>,
        queue_limit: Option<usize>,
    ) -> Result<Self, JsValue> {
        let resolved_kind = kind.unwrap_or_else(|| "inline".to_string());
        if !matches!(resolved_kind.as_str(), "inline" | "main" | "thread") {
            return Err(JsValue::from_str(
                "callback placement kind must be inline, main, or thread",
            ));
        }
        if resolved_kind == "thread" {
            require_thread_name(thread_name.as_deref())?;
        }
        let resolved_queue_limit = queue_limit.unwrap_or(DEFAULT_CALLBACK_QUEUE_LIMIT);
        if resolved_queue_limit == 0 {
            return Err(JsValue::from_str("queueLimit must be positive"));
        }
        Ok(Self {
            kind: resolved_kind,
            thread_name,
            queue_limit: resolved_queue_limit,
        })
    }

    pub fn inline() -> CallbackPlacement {
        CallbackPlacement {
            kind: "inline".to_string(),
            thread_name: None,
            queue_limit: DEFAULT_CALLBACK_QUEUE_LIMIT,
        }
    }

    #[wasm_bindgen(js_name = mainThread)]
    pub fn main_thread(queue_limit: Option<usize>) -> Result<CallbackPlacement, JsValue> {
        CallbackPlacement::new(Some("main".to_string()), None, queue_limit)
    }

    #[wasm_bindgen(js_name = spawnedThread)]
    pub fn spawned_thread(
        name: String,
        queue_limit: Option<usize>,
    ) -> Result<CallbackPlacement, JsValue> {
        CallbackPlacement::new(
            Some("thread".to_string()),
            Some(require_thread_name(Some(&name))?.to_string()),
            queue_limit,
        )
    }

    #[wasm_bindgen(getter)]
    pub fn kind(&self) -> String {
        self.kind.clone()
    }

    #[wasm_bindgen(getter, js_name = threadName)]
    pub fn thread_name(&self) -> Option<String> {
        self.thread_name.clone()
    }

    #[wasm_bindgen(getter, js_name = queueLimit)]
    pub fn queue_limit(&self) -> usize {
        self.queue_limit
    }
}

#[wasm_bindgen]
impl PubSubCallbackSubscription {
    #[wasm_bindgen(getter, js_name = isDisposed)]
    pub fn is_disposed(&self) -> bool {
        self.is_disposed
    }

    pub fn dispose(&mut self) -> Result<bool, JsValue> {
        if self.is_disposed {
            return Ok(false);
        }
        let removed = self
            .callbacks
            .borrow_mut()
            .callbacks
            .remove(&self.id)
            .is_some();
        let _ = self.proxy.handle(
            WorkerOperation::Unsubscribe,
            Vec::new(),
            BTreeMap::from([("subscription".to_string(), self.subscription_name.clone())]),
        )?;
        self.is_disposed = true;
        Ok(removed)
    }
}

#[wasm_bindgen]
pub struct Lock {
    name: String,
    proxy: WorkerProxy,
}

#[wasm_bindgen]
impl Lock {
    #[wasm_bindgen(constructor)]
    pub fn new(name: String, retained_messages: Option<usize>) -> Result<Self, JsValue> {
        Ok(WorkerProxy::new(retained_messages.unwrap_or(DEFAULT_RETAINED_MESSAGES))?.lock(name))
    }

    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn take(&mut self, owner: Option<String>, blocking: bool) -> Result<LockLease, JsValue> {
        let mut arguments = BTreeMap::from([("blocking".to_string(), blocking.to_string())]);
        if let Some(owner) = owner {
            arguments.insert("owner".to_string(), owner);
        }
        let response = self.proxy.handle_endpoint(
            WorkerEndpointKind::Lock,
            self.name.clone(),
            WorkerOperation::Take,
            Vec::new(),
            arguments,
        )?;
        Ok(LockLease {
            lock_name: self.name.clone(),
            owner: required_metadata(&response, "owner")?.to_string(),
            acquired_time_ns: required_metadata(&response, "acquired_time_ns")?.to_string(),
            is_released: false,
            proxy: self.proxy.clone(),
        })
    }
}

#[wasm_bindgen]
pub struct LockLease {
    lock_name: String,
    owner: String,
    acquired_time_ns: String,
    is_released: bool,
    proxy: WorkerProxy,
}

#[wasm_bindgen]
impl LockLease {
    #[wasm_bindgen(getter, js_name = lockName)]
    pub fn lock_name(&self) -> String {
        self.lock_name.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn owner(&self) -> String {
        self.owner.clone()
    }

    #[wasm_bindgen(getter, js_name = acquiredTimeNs)]
    pub fn acquired_time_ns(&self) -> String {
        self.acquired_time_ns.clone()
    }

    #[wasm_bindgen(getter, js_name = isReleased)]
    pub fn is_released(&self) -> bool {
        self.is_released
    }

    pub fn release(&mut self) -> Result<bool, JsValue> {
        if self.is_released {
            return Ok(false);
        }
        let response = self.proxy.handle_endpoint(
            WorkerEndpointKind::Lock,
            self.lock_name.clone(),
            WorkerOperation::Release,
            Vec::new(),
            BTreeMap::new(),
        )?;
        let released = required_metadata(&response, "released")? == "true";
        if released {
            self.is_released = true;
        }
        Ok(released)
    }
}

#[wasm_bindgen]
pub struct Clock {
    name: String,
    proxy: WorkerProxy,
}

#[wasm_bindgen]
impl Clock {
    #[wasm_bindgen(constructor)]
    pub fn new(name: String, retained_messages: Option<usize>) -> Result<Self, JsValue> {
        Ok(WorkerProxy::new(retained_messages.unwrap_or(DEFAULT_RETAINED_MESSAGES))?.clock(name))
    }

    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn tick(&mut self) -> Result<u64, JsValue> {
        let response = self.proxy.handle_endpoint(
            WorkerEndpointKind::Clock,
            self.name.clone(),
            WorkerOperation::Tick,
            Vec::new(),
            BTreeMap::new(),
        )?;
        required_metadata(&response, "tick")?
            .parse::<u64>()
            .map_err(|error| JsValue::from_str(&format!("invalid clock tick: {error}")))
    }

    #[wasm_bindgen(js_name = nowNs)]
    pub fn now_ns(&mut self) -> Result<String, JsValue> {
        let response = self.proxy.handle_endpoint(
            WorkerEndpointKind::Clock,
            self.name.clone(),
            WorkerOperation::Now,
            Vec::new(),
            BTreeMap::new(),
        )?;
        Ok(required_metadata(&response, "now_ns")?.to_string())
    }
}

#[wasm_bindgen]
impl PubSub {
    #[wasm_bindgen(constructor)]
    pub fn new(topic: String, retained_messages: Option<usize>) -> Result<Self, JsValue> {
        Ok(WorkerProxy::new(retained_messages.unwrap_or(DEFAULT_RETAINED_MESSAGES))?.pubsub(topic))
    }

    #[wasm_bindgen(getter)]
    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    pub fn lock(&self) -> Lock {
        self.proxy.lock(pubsub_infrastructure_lock(&self.topic))
    }

    pub fn clock(&self) -> Clock {
        self.proxy.clock(pubsub_infrastructure_clock(&self.topic))
    }

    #[wasm_bindgen(js_name = setDesktopSpawner)]
    pub fn set_desktop_spawner(&self, spawner: &Function) {
        self.proxy.set_desktop_spawner(spawner);
    }

    #[wasm_bindgen(js_name = clearDesktopSpawner)]
    pub fn clear_desktop_spawner(&self) {
        self.proxy.clear_desktop_spawner();
    }

    #[wasm_bindgen(js_name = spawnRustWorker)]
    pub fn spawn_rust_worker(&self, command: String, args: Array) -> Result<JsValue, JsValue> {
        self.proxy.spawn_rust_worker(command, args)
    }

    pub fn subscribe(
        &mut self,
        name: Option<String>,
        replay_from_beginning: bool,
    ) -> Result<JsValue, JsValue> {
        let mut arguments = BTreeMap::from([("topic".to_string(), self.topic.clone())]);
        if let Some(name) = name {
            arguments.insert("name".to_string(), name);
        }
        if replay_from_beginning {
            arguments.insert("replay_from_beginning".to_string(), "true".to_string());
        }
        let response = self
            .proxy
            .handle(WorkerOperation::Subscribe, Vec::new(), arguments)?;
        let value = Object::new();
        set_string(&value, "name", required_metadata(&response, "name")?)?;
        set_string(&value, "topic", required_metadata(&response, "topic")?)?;
        Ok(value.into())
    }

    pub fn unsubscribe(&mut self, name: String) -> Result<bool, JsValue> {
        let response = self.proxy.handle(
            WorkerOperation::Unsubscribe,
            Vec::new(),
            BTreeMap::from([("subscription".to_string(), name)]),
        )?;
        Ok(required_metadata(&response, "removed")? == "true")
    }

    pub fn publish(&mut self, payload: Vec<u8>) -> Result<JsValue, JsValue> {
        let response = self.proxy.handle(
            WorkerOperation::Publish,
            payload,
            BTreeMap::from([("topic".to_string(), self.topic.clone())]),
        )?;
        self.deliver_callbacks()?;
        delivery_to_js(response)
    }

    #[wasm_bindgen(js_name = subscribeCallback)]
    pub fn subscribe_callback(
        &mut self,
        callback: &Function,
        placement: Option<CallbackPlacement>,
        replay_latest: Option<bool>,
    ) -> Result<PubSubCallbackSubscription, JsValue> {
        self.callback(callback, placement, replay_latest)
    }

    pub fn callback(
        &mut self,
        callback: &Function,
        placement: Option<CallbackPlacement>,
        replay_latest: Option<bool>,
    ) -> Result<PubSubCallbackSubscription, JsValue> {
        let callback_id = {
            let mut state = self.callbacks.borrow_mut();
            let callback_id = state.next_callback_id;
            state.next_callback_id += 1;
            let subscription_name = format!("{}:callback:{callback_id}", self.topic);
            state.callbacks.insert(
                callback_id,
                PubSubCallback {
                    callback: callback.clone(),
                    placement: placement.unwrap_or_else(CallbackPlacement::inline),
                    subscription_name: subscription_name.clone(),
                    pending: 0,
                },
            );
            callback_id
        };
        let subscription_name = self
            .callbacks
            .borrow()
            .callbacks
            .get(&callback_id)
            .expect("callback was just inserted")
            .subscription_name
            .clone();
        self.subscribe(Some(subscription_name.clone()), false)?;
        if replay_latest.unwrap_or(false) {
            if let Some(message) = self.latest_callback_message()? {
                self.deliver_callback_message(callback_id, message)?;
            }
        }
        Ok(PubSubCallbackSubscription {
            id: callback_id,
            subscription_name,
            callbacks: self.callbacks.clone(),
            is_disposed: false,
            proxy: self.proxy.clone(),
        })
    }

    pub fn poll(
        &mut self,
        subscription: String,
        max_messages: Option<usize>,
    ) -> Result<Array, JsValue> {
        let mut arguments = BTreeMap::from([("subscription".to_string(), subscription)]);
        if let Some(max_messages) = max_messages {
            arguments.insert("max_messages".to_string(), max_messages.to_string());
        }
        let response = self
            .proxy
            .handle(WorkerOperation::Poll, Vec::new(), arguments)?;
        payloads_to_js(response)
    }

    pub fn latest(&mut self) -> Result<JsValue, JsValue> {
        let response = self.proxy.handle(
            WorkerOperation::Latest,
            Vec::new(),
            BTreeMap::from([("topic".to_string(), self.topic.clone())]),
        )?;
        if response.payloads.is_empty() {
            return Ok(JsValue::NULL);
        }
        payload_to_js(
            response
                .payloads
                .first()
                .expect("checked payloads are not empty"),
            response.metadata.get("topic").map(String::as_str),
            response.metadata.get("offset").map(String::as_str),
        )
    }

    #[wasm_bindgen(js_name = messageCount)]
    pub fn message_count(&self) -> usize {
        self.proxy.message_count()
    }

    #[wasm_bindgen(js_name = subscriberCount)]
    pub fn subscriber_count(&self) -> usize {
        self.proxy.subscriber_count()
    }
}

impl PubSub {
    fn deliver_callbacks(&mut self) -> Result<(), JsValue> {
        let callback_ids = self.callbacks.borrow().callback_ids();
        for callback_id in callback_ids {
            let subscription_name = match self.callbacks.borrow().callbacks.get(&callback_id) {
                Some(callback) => callback.subscription_name.clone(),
                None => continue,
            };
            for message in self.poll(subscription_name, None)?.iter() {
                self.deliver_callback_message(callback_id, message)?;
            }
        }
        Ok(())
    }

    fn deliver_callback_message(
        &mut self,
        callback_id: u64,
        message: JsValue,
    ) -> Result<(), JsValue> {
        let delivery = {
            let mut state = self.callbacks.borrow_mut();
            let Some(callback) = state.callbacks.get_mut(&callback_id) else {
                return Ok(());
            };
            if callback.pending >= callback.placement.queue_limit {
                return Err(JsValue::from_str("PubSub.callback callback queue is full"));
            }
            callback.pending += 1;
            (callback.callback.clone(), callback.placement.clone())
        };
        deliver_js_callback(
            delivery.0,
            message,
            delivery.1,
            self.callbacks.clone(),
            callback_id,
        )
    }

    fn latest_callback_message(&mut self) -> Result<Option<JsValue>, JsValue> {
        let response = self.proxy.handle(
            WorkerOperation::Latest,
            Vec::new(),
            BTreeMap::from([("topic".to_string(), self.topic.clone())]),
        )?;
        if response.payloads.is_empty() {
            return Ok(None);
        }
        Ok(Some(payload_to_js(
            response
                .payloads
                .first()
                .expect("checked payloads are not empty"),
            response.metadata.get("topic").map(String::as_str),
            response.metadata.get("offset").map(String::as_str),
        )?))
    }
}

struct WorkerProxyState {
    proxy: WorkerProxyCore,
    raw_endpoint: WorkerEndpointRefCore,
    desktop_spawner: Option<Function>,
    next_request_id: u64,
}

#[derive(Default)]
struct PubSubCallbackState {
    callbacks: BTreeMap<u64, PubSubCallback>,
    next_callback_id: u64,
}

impl PubSubCallbackState {
    fn callback_ids(&self) -> Vec<u64> {
        self.callbacks.keys().copied().collect()
    }
}

struct PubSubCallback {
    callback: Function,
    placement: CallbackPlacement,
    subscription_name: String,
    pending: usize,
}

impl WorkerProxy {
    fn handle_endpoint(
        &self,
        kind: WorkerEndpointKind,
        endpoint_id: String,
        operation: WorkerOperation,
        payload: Vec<u8>,
        arguments: BTreeMap<String, String>,
    ) -> Result<WorkerResponseCore, JsValue> {
        let mut state = self.state.borrow_mut();
        let request_id = format!("wasm-request-{}", state.next_request_id);
        state.next_request_id += 1;
        let endpoint = WorkerEndpointRefCore {
            runtime_id: state.raw_endpoint.runtime_id.clone(),
            endpoint_id,
            kind,
        };
        state
            .proxy
            .handle(WorkerRequestCore {
                request_id,
                endpoint,
                operation,
                payload,
                arguments,
            })
            .map_err(js_error)
    }

    fn handle(
        &self,
        operation: WorkerOperation,
        payload: Vec<u8>,
        arguments: BTreeMap<String, String>,
    ) -> Result<WorkerResponseCore, JsValue> {
        let endpoint = self.state.borrow().raw_endpoint.clone();
        self.handle_endpoint(
            endpoint.kind,
            endpoint.endpoint_id,
            operation,
            payload,
            arguments,
        )
    }
}

fn delivery_to_js(response: WorkerResponseCore) -> Result<JsValue, JsValue> {
    let value = Object::new();
    set_string(&value, "topic", required_metadata(&response, "topic")?)?;
    set_string(&value, "offset", required_metadata(&response, "offset")?)?;
    let delivered_to = Array::new();
    if let Some(subscribers) = response.metadata.get("delivered_to") {
        for subscriber in subscribers.split(',').filter(|value| !value.is_empty()) {
            delivered_to.push(&JsValue::from_str(subscriber));
        }
    }
    Reflect::set(
        &value,
        &JsValue::from_str("deliveredTo"),
        delivered_to.as_ref(),
    )?;
    Ok(value.into())
}

fn payloads_to_js(response: WorkerResponseCore) -> Result<Array, JsValue> {
    let offsets = response
        .metadata
        .get("offsets")
        .map(|offsets| offsets.split(',').collect::<Vec<_>>())
        .unwrap_or_default();
    let values = Array::new();
    for (index, payload) in response.payloads.iter().enumerate() {
        let offset = offsets.get(index).copied();
        values.push(&payload_to_js(payload, None, offset)?);
    }
    Ok(values)
}

fn payload_to_js(
    payload: &[u8],
    topic: Option<&str>,
    offset: Option<&str>,
) -> Result<JsValue, JsValue> {
    let value = Object::new();
    if let Some(topic) = topic {
        set_string(&value, "topic", topic)?;
    }
    if let Some(offset) = offset {
        set_string(&value, "offset", offset)?;
    }
    Reflect::set(
        &value,
        &JsValue::from_str("payload"),
        Uint8Array::from(payload).as_ref(),
    )?;
    Ok(value.into())
}

fn set_string(target: &Object, name: &str, value: &str) -> Result<(), JsValue> {
    Reflect::set(target, &JsValue::from_str(name), &JsValue::from_str(value))?;
    Ok(())
}

fn pubsub_infrastructure_lock(topic: &str) -> String {
    format!("{topic}.infrastructure.lock")
}

fn pubsub_infrastructure_clock(topic: &str) -> String {
    format!("{topic}.infrastructure.clock")
}

fn deliver_js_callback(
    callback: Function,
    message: JsValue,
    placement: CallbackPlacement,
    callbacks: Rc<RefCell<PubSubCallbackState>>,
    callback_id: u64,
) -> Result<(), JsValue> {
    match placement.kind.as_str() {
        "inline" => run_js_callback(callback, message, callbacks, callback_id),
        "main" => schedule_js_callback("queueMicrotask", callback, message, callbacks, callback_id),
        "thread" => schedule_js_timeout(callback, message, callbacks, callback_id),
        _ => Err(JsValue::from_str(
            "callback placement kind must be inline, main, or thread",
        )),
    }
}

fn schedule_js_callback(
    scheduler_name: &str,
    callback: Function,
    message: JsValue,
    callbacks: Rc<RefCell<PubSubCallbackState>>,
    callback_id: u64,
) -> Result<(), JsValue> {
    let global = js_sys::global();
    let scheduler = Reflect::get(&global, &JsValue::from_str(scheduler_name))?;
    if !scheduler.is_function() {
        return run_js_callback(callback, message, callbacks, callback_id);
    }
    let scheduler = scheduler.dyn_into::<Function>()?;
    let closure = Closure::once_into_js(move || {
        if let Err(error) = run_js_callback(callback, message, callbacks, callback_id) {
            wasm_bindgen::throw_val(error);
        }
    });
    scheduler.call1(&global, &closure)?;
    Ok(())
}

fn schedule_js_timeout(
    callback: Function,
    message: JsValue,
    callbacks: Rc<RefCell<PubSubCallbackState>>,
    callback_id: u64,
) -> Result<(), JsValue> {
    let global = js_sys::global();
    let scheduler = Reflect::get(&global, &JsValue::from_str("setTimeout"))?;
    if !scheduler.is_function() {
        return schedule_js_callback("queueMicrotask", callback, message, callbacks, callback_id);
    }
    let scheduler = scheduler.dyn_into::<Function>()?;
    let closure = Closure::once_into_js(move || {
        if let Err(error) = run_js_callback(callback, message, callbacks, callback_id) {
            wasm_bindgen::throw_val(error);
        }
    });
    scheduler.call2(&global, &closure, &JsValue::from_f64(0.0))?;
    Ok(())
}

fn run_js_callback(
    callback: Function,
    message: JsValue,
    callbacks: Rc<RefCell<PubSubCallbackState>>,
    callback_id: u64,
) -> Result<(), JsValue> {
    let result = callback.call1(&JsValue::UNDEFINED, &message);
    clear_pending_callback(callbacks, callback_id);
    result.map(|_| ())
}

fn clear_pending_callback(callbacks: Rc<RefCell<PubSubCallbackState>>, callback_id: u64) {
    if let Some(callback) = callbacks.borrow_mut().callbacks.get_mut(&callback_id) {
        callback.pending = callback.pending.saturating_sub(1);
    }
}

fn require_thread_name(value: Option<&str>) -> Result<&str, JsValue> {
    let Some(value) = value else {
        return Err(JsValue::from_str("thread name must be a non-empty string"));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(JsValue::from_str("thread name must be a non-empty string"));
    }
    Ok(trimmed)
}

fn required_metadata<'a>(response: &'a WorkerResponseCore, name: &str) -> Result<&'a str, JsValue> {
    response
        .metadata
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| JsValue::from_str(&format!("worker response missing metadata: {name}")))
}

fn js_error(message: String) -> JsValue {
    JsValue::from_str(&message)
}
