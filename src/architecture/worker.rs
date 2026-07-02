use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::architecture::{InMemoryPubSubCore, PubSubMessageCore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeKind {
    PubSub,
}

impl RuntimeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeKind::PubSub => "pubsub",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerEndpointKind {
    RawData,
    Sql,
    Variable,
    Lock,
    Clock,
}

impl WorkerEndpointKind {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerEndpointKind::RawData => "raw_data",
            WorkerEndpointKind::Sql => "sql",
            WorkerEndpointKind::Variable => "variable",
            WorkerEndpointKind::Lock => "lock",
            WorkerEndpointKind::Clock => "clock",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerInfrastructureKind {
    InProcess,
    Process,
    Proxy,
}

impl WorkerInfrastructureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerInfrastructureKind::InProcess => "in_process",
            WorkerInfrastructureKind::Process => "process",
            WorkerInfrastructureKind::Proxy => "proxy",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerTransportKind {
    Direct,
    Ipc,
    Http,
    WebSocket,
    MessagePort,
}

impl WorkerTransportKind {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerTransportKind::Direct => "direct",
            WorkerTransportKind::Ipc => "ipc",
            WorkerTransportKind::Http => "http",
            WorkerTransportKind::WebSocket => "web_socket",
            WorkerTransportKind::MessagePort => "message_port",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerOperation {
    Publish,
    Subscribe,
    Unsubscribe,
    Poll,
    Latest,
    Query,
    Read,
    Write,
    Take,
    Release,
    Tick,
    Now,
}

impl WorkerOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerOperation::Publish => "publish",
            WorkerOperation::Subscribe => "subscribe",
            WorkerOperation::Unsubscribe => "unsubscribe",
            WorkerOperation::Poll => "poll",
            WorkerOperation::Latest => "latest",
            WorkerOperation::Query => "query",
            WorkerOperation::Read => "read",
            WorkerOperation::Write => "write",
            WorkerOperation::Take => "take",
            WorkerOperation::Release => "release",
            WorkerOperation::Tick => "tick",
            WorkerOperation::Now => "now",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeProfileCore {
    pub runtime_id: String,
    pub kind: RuntimeKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerProxyProfileCore {
    pub infrastructure_id: String,
    pub kind: WorkerInfrastructureKind,
    pub transport: WorkerTransportKind,
    pub address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerEndpointRefCore {
    pub runtime_id: String,
    pub endpoint_id: String,
    pub kind: WorkerEndpointKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerRequestCore {
    pub request_id: String,
    pub endpoint: WorkerEndpointRefCore,
    pub operation: WorkerOperation,
    pub payload: Vec<u8>,
    pub arguments: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerResponseCore {
    pub request_id: String,
    pub endpoint: WorkerEndpointRefCore,
    pub payloads: Vec<Vec<u8>>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct RuntimeCore {
    profile: RuntimeProfileCore,
    pubsub: InMemoryPubSubCore,
    locks: BTreeMap<String, LockStateCore>,
    logical_clock: u64,
}

impl RuntimeCore {
    pub fn pubsub(runtime_id: String, retained_messages: usize) -> Result<Self, String> {
        validate_identifier(&runtime_id, "runtime_id")?;
        Ok(Self {
            profile: RuntimeProfileCore {
                runtime_id,
                kind: RuntimeKind::PubSub,
            },
            pubsub: InMemoryPubSubCore::new(retained_messages)?,
            locks: BTreeMap::new(),
            logical_clock: 0,
        })
    }

    pub fn profile(&self) -> &RuntimeProfileCore {
        &self.profile
    }

    pub fn retained_messages(&self) -> usize {
        self.pubsub.retained_messages()
    }

    pub fn message_count(&self) -> usize {
        self.pubsub.message_count()
    }

    pub fn subscriber_count(&self) -> usize {
        self.pubsub.subscriber_count()
    }

    pub fn handle(&mut self, request: WorkerRequestCore) -> Result<WorkerResponseCore, String> {
        if request.endpoint.runtime_id != self.profile.runtime_id {
            return Err(format!(
                "request runtime_id {} does not match runtime {}",
                request.endpoint.runtime_id, self.profile.runtime_id
            ));
        }
        match request.endpoint.kind {
            WorkerEndpointKind::RawData => self.handle_raw_pubsub(request),
            WorkerEndpointKind::Sql => Err("sql worker endpoint is not configured".to_string()),
            WorkerEndpointKind::Variable => {
                Err("variable worker endpoint is not configured".to_string())
            }
            WorkerEndpointKind::Lock => self.handle_lock(request),
            WorkerEndpointKind::Clock => self.handle_clock(request),
        }
    }

    fn handle_raw_pubsub(
        &mut self,
        request: WorkerRequestCore,
    ) -> Result<WorkerResponseCore, String> {
        match request.operation {
            WorkerOperation::Publish => self.handle_publish(request),
            WorkerOperation::Subscribe => self.handle_subscribe(request),
            WorkerOperation::Unsubscribe => self.handle_unsubscribe(request),
            WorkerOperation::Poll => self.handle_poll(request),
            WorkerOperation::Latest => self.handle_latest(request),
            operation => Err(format!(
                "operation {} is not supported by raw pubsub endpoint",
                operation.as_str()
            )),
        }
    }

    fn handle_publish(&mut self, request: WorkerRequestCore) -> Result<WorkerResponseCore, String> {
        let topic = required_argument(&request.arguments, "topic")?;
        let delivery = self.pubsub.publish(topic.to_string(), request.payload)?;
        let mut metadata = BTreeMap::new();
        metadata.insert("topic".to_string(), delivery.topic);
        metadata.insert("offset".to_string(), delivery.offset.to_string());
        metadata.insert("delivered_to".to_string(), delivery.delivered_to.join(","));
        Ok(WorkerResponseCore {
            request_id: request.request_id,
            endpoint: request.endpoint,
            payloads: Vec::new(),
            metadata,
        })
    }

    fn handle_subscribe(
        &mut self,
        request: WorkerRequestCore,
    ) -> Result<WorkerResponseCore, String> {
        let topic = required_argument(&request.arguments, "topic")?;
        let name = optional_argument(&request.arguments, "name").map(str::to_string);
        let replay_from_beginning = optional_argument(&request.arguments, "replay_from_beginning")
            .is_some_and(|value| value == "true");
        let subscription = self
            .pubsub
            .subscribe(topic.to_string(), name, replay_from_beginning)?;
        let mut metadata = BTreeMap::new();
        metadata.insert("name".to_string(), subscription.name);
        metadata.insert("topic".to_string(), subscription.topic);
        Ok(WorkerResponseCore {
            request_id: request.request_id,
            endpoint: request.endpoint,
            payloads: Vec::new(),
            metadata,
        })
    }

    fn handle_poll(&mut self, request: WorkerRequestCore) -> Result<WorkerResponseCore, String> {
        let subscription = required_argument(&request.arguments, "subscription")?;
        let max_messages = optional_argument(&request.arguments, "max_messages")
            .map(parse_positive_usize)
            .transpose()?;
        let messages = self.pubsub.poll(subscription, max_messages)?;
        let mut metadata = BTreeMap::new();
        metadata.insert("offsets".to_string(), message_offsets(&messages));
        Ok(WorkerResponseCore {
            request_id: request.request_id,
            endpoint: request.endpoint,
            payloads: messages
                .into_iter()
                .map(|message| message.payload)
                .collect(),
            metadata,
        })
    }

    fn handle_unsubscribe(
        &mut self,
        request: WorkerRequestCore,
    ) -> Result<WorkerResponseCore, String> {
        let subscription = required_argument(&request.arguments, "subscription")?;
        let removed = self.pubsub.unsubscribe(subscription)?;
        let mut metadata = BTreeMap::new();
        metadata.insert("removed".to_string(), removed.to_string());
        Ok(WorkerResponseCore {
            request_id: request.request_id,
            endpoint: request.endpoint,
            payloads: Vec::new(),
            metadata,
        })
    }

    fn handle_latest(&self, request: WorkerRequestCore) -> Result<WorkerResponseCore, String> {
        let topic = optional_argument(&request.arguments, "topic");
        let message = self.pubsub.latest(topic)?;
        let mut metadata = BTreeMap::new();
        let payloads = match message {
            Some(message) => {
                metadata.insert("topic".to_string(), message.topic);
                metadata.insert("offset".to_string(), message.offset.to_string());
                vec![message.payload]
            }
            None => Vec::new(),
        };
        Ok(WorkerResponseCore {
            request_id: request.request_id,
            endpoint: request.endpoint,
            payloads,
            metadata,
        })
    }

    fn handle_lock(&mut self, request: WorkerRequestCore) -> Result<WorkerResponseCore, String> {
        match request.operation {
            WorkerOperation::Take => self.handle_lock_take(request),
            WorkerOperation::Release => self.handle_lock_release(request),
            operation => Err(format!(
                "operation {} is not supported by lock endpoint",
                operation.as_str()
            )),
        }
    }

    fn handle_lock_take(
        &mut self,
        request: WorkerRequestCore,
    ) -> Result<WorkerResponseCore, String> {
        let lock_name = request.endpoint.endpoint_id.clone();
        let owner = optional_argument(&request.arguments, "owner").unwrap_or("worker");
        let blocking = optional_argument(&request.arguments, "blocking") != Some("false");
        let state = self.locks.entry(lock_name).or_default();
        if state.owner.is_some() && !blocking {
            return Err("lock is already held".to_string());
        }
        if state.owner.is_some() {
            return Err("blocking lock waits are not supported by this runtime".to_string());
        }
        state.owner = Some(owner.to_string());
        state.acquired_time_ns = current_time_ns()?;

        let mut metadata = BTreeMap::new();
        metadata.insert("owner".to_string(), owner.to_string());
        metadata.insert(
            "acquired_time_ns".to_string(),
            state.acquired_time_ns.to_string(),
        );
        Ok(WorkerResponseCore {
            request_id: request.request_id,
            endpoint: request.endpoint,
            payloads: Vec::new(),
            metadata,
        })
    }

    fn handle_lock_release(
        &mut self,
        request: WorkerRequestCore,
    ) -> Result<WorkerResponseCore, String> {
        let lock_name = request.endpoint.endpoint_id.clone();
        let released = self
            .locks
            .get_mut(&lock_name)
            .is_some_and(|state| state.owner.take().is_some());
        let mut metadata = BTreeMap::new();
        metadata.insert("released".to_string(), released.to_string());
        Ok(WorkerResponseCore {
            request_id: request.request_id,
            endpoint: request.endpoint,
            payloads: Vec::new(),
            metadata,
        })
    }

    fn handle_clock(&mut self, request: WorkerRequestCore) -> Result<WorkerResponseCore, String> {
        match request.operation {
            WorkerOperation::Tick => {
                self.logical_clock += 1;
                let mut metadata = BTreeMap::new();
                metadata.insert("tick".to_string(), self.logical_clock.to_string());
                Ok(WorkerResponseCore {
                    request_id: request.request_id,
                    endpoint: request.endpoint,
                    payloads: Vec::new(),
                    metadata,
                })
            }
            WorkerOperation::Now => {
                let mut metadata = BTreeMap::new();
                metadata.insert("now_ns".to_string(), current_time_ns()?.to_string());
                Ok(WorkerResponseCore {
                    request_id: request.request_id,
                    endpoint: request.endpoint,
                    payloads: Vec::new(),
                    metadata,
                })
            }
            operation => Err(format!(
                "operation {} is not supported by clock endpoint",
                operation.as_str()
            )),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct LockStateCore {
    owner: Option<String>,
    acquired_time_ns: u128,
}

#[derive(Clone, Debug)]
pub struct WorkerProxyCore {
    profile: WorkerProxyProfileCore,
    runtime: RuntimeCore,
}

impl WorkerProxyCore {
    pub fn pubsub(runtime_id: String, retained_messages: usize) -> Result<Self, String> {
        let profile = WorkerProxyProfileCore::in_process(runtime_id.as_str())?;
        Self::pubsub_with_profile(runtime_id, retained_messages, profile)
    }

    pub fn pubsub_with_profile(
        runtime_id: String,
        retained_messages: usize,
        profile: WorkerProxyProfileCore,
    ) -> Result<Self, String> {
        profile.validate()?;
        Ok(Self {
            profile,
            runtime: RuntimeCore::pubsub(runtime_id, retained_messages)?,
        })
    }

    pub fn profile(&self) -> &WorkerProxyProfileCore {
        &self.profile
    }

    pub fn runtime_profile(&self) -> &RuntimeProfileCore {
        self.runtime.profile()
    }

    pub fn retained_messages(&self) -> usize {
        self.runtime.retained_messages()
    }

    pub fn message_count(&self) -> usize {
        self.runtime.message_count()
    }

    pub fn subscriber_count(&self) -> usize {
        self.runtime.subscriber_count()
    }

    pub fn configure_profile(&mut self, profile: WorkerProxyProfileCore) -> Result<(), String> {
        profile.validate()?;
        self.profile = profile;
        Ok(())
    }

    pub fn handle(&mut self, request: WorkerRequestCore) -> Result<WorkerResponseCore, String> {
        self.runtime.handle(request)
    }
}

impl WorkerProxyProfileCore {
    pub fn in_process(runtime_id: &str) -> Result<Self, String> {
        validate_identifier(runtime_id, "runtime_id")?;
        Ok(Self {
            infrastructure_id: format!("{runtime_id}:in-process"),
            kind: WorkerInfrastructureKind::InProcess,
            transport: WorkerTransportKind::Direct,
            address: format!("memory://{runtime_id}"),
        })
    }

    pub fn proxy(infrastructure_id: String, address: String) -> Result<Self, String> {
        let profile = Self {
            infrastructure_id,
            kind: WorkerInfrastructureKind::Proxy,
            transport: WorkerTransportKind::WebSocket,
            address,
        };
        profile.validate()?;
        Ok(profile)
    }

    fn validate(&self) -> Result<(), String> {
        validate_identifier(&self.infrastructure_id, "infrastructure_id")?;
        validate_identifier(&self.address, "address")
    }
}

fn message_offsets(messages: &[PubSubMessageCore]) -> String {
    messages
        .iter()
        .map(|message| message.offset.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn required_argument<'a>(
    arguments: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, String> {
    optional_argument(arguments, name).ok_or_else(|| format!("missing worker argument: {name}"))
}

fn optional_argument<'a>(arguments: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    arguments
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid positive integer {value:?}: {error}"))?;
    if parsed == 0 {
        return Err("positive integer must be greater than zero".to_string());
    }
    Ok(parsed)
}

fn validate_identifier(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must be a non-empty string"));
    }
    Ok(())
}

fn current_time_ns() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(|error| format!("system clock before UNIX epoch: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_pubsub_request_flow_uses_bytes_and_endpoint_identity() {
        let mut runtime =
            RuntimeCore::pubsub("runtime-a".to_string(), 8).expect("runtime can be created");
        let endpoint = WorkerEndpointRefCore {
            runtime_id: "runtime-a".to_string(),
            endpoint_id: "events.raw".to_string(),
            kind: WorkerEndpointKind::RawData,
        };

        let subscribe = runtime
            .handle(WorkerRequestCore {
                request_id: "subscribe-1".to_string(),
                endpoint: endpoint.clone(),
                operation: WorkerOperation::Subscribe,
                payload: Vec::new(),
                arguments: BTreeMap::from([
                    ("topic".to_string(), "heart.input".to_string()),
                    ("name".to_string(), "heart".to_string()),
                ]),
            })
            .expect("subscription succeeds");
        let publish = runtime
            .handle(WorkerRequestCore {
                request_id: "publish-1".to_string(),
                endpoint: endpoint.clone(),
                operation: WorkerOperation::Publish,
                payload: b"pressed".to_vec(),
                arguments: BTreeMap::from([("topic".to_string(), "heart.input".to_string())]),
            })
            .expect("publish succeeds");
        let poll = runtime
            .handle(WorkerRequestCore {
                request_id: "poll-1".to_string(),
                endpoint,
                operation: WorkerOperation::Poll,
                payload: Vec::new(),
                arguments: BTreeMap::from([("subscription".to_string(), "heart".to_string())]),
            })
            .expect("poll succeeds");

        assert_eq!(subscribe.metadata["name"], "heart");
        assert_eq!(publish.metadata["delivered_to"], "heart");
        assert_eq!(poll.payloads, vec![b"pressed".to_vec()]);
        assert_eq!(poll.metadata["offsets"], "0");
    }

    #[test]
    fn sql_endpoint_is_explicitly_separate_from_raw_pubsub() {
        let mut runtime = RuntimeCore::pubsub("runtime-a".to_string(), 8).expect("valid runtime");

        let error = runtime
            .handle(WorkerRequestCore {
                request_id: "query-1".to_string(),
                endpoint: WorkerEndpointRefCore {
                    runtime_id: "runtime-a".to_string(),
                    endpoint_id: "events.sql".to_string(),
                    kind: WorkerEndpointKind::Sql,
                },
                operation: WorkerOperation::Query,
                payload: b"SELECT * FROM stream".to_vec(),
                arguments: BTreeMap::new(),
            })
            .expect_err("sql is not configured on the raw runtime");

        assert_eq!(error, "sql worker endpoint is not configured");
    }

    #[test]
    fn infrastructure_can_change_without_changing_endpoint_requests() {
        let mut proxy = WorkerProxyCore::pubsub("runtime-a".to_string(), 8).expect("valid proxy");
        let endpoint = WorkerEndpointRefCore {
            runtime_id: "runtime-a".to_string(),
            endpoint_id: "events.raw".to_string(),
            kind: WorkerEndpointKind::RawData,
        };
        let profile = WorkerProxyProfileCore::proxy(
            "browser-proxy".to_string(),
            "ws://manyfold.local/runtime-a".to_string(),
        )
        .expect("valid proxy profile");

        proxy
            .configure_profile(profile)
            .expect("infrastructure can be delegated to proxy");
        let response = proxy
            .handle(WorkerRequestCore {
                request_id: "publish-1".to_string(),
                endpoint,
                operation: WorkerOperation::Publish,
                payload: b"shared queue payload".to_vec(),
                arguments: BTreeMap::from([("topic".to_string(), "heart.input".to_string())]),
            })
            .expect("same endpoint request still works");

        assert_eq!(proxy.profile().kind, WorkerInfrastructureKind::Proxy);
        assert_eq!(proxy.profile().transport.as_str(), "web_socket");
        assert_eq!(response.metadata["topic"], "heart.input");
    }

    #[test]
    fn lock_endpoint_takes_and_releases_named_lock() {
        let mut runtime = RuntimeCore::pubsub("runtime-a".to_string(), 8).expect("valid runtime");
        let endpoint = WorkerEndpointRefCore {
            runtime_id: "runtime-a".to_string(),
            endpoint_id: "heart.input.lock".to_string(),
            kind: WorkerEndpointKind::Lock,
        };

        let lease = runtime
            .handle(WorkerRequestCore {
                request_id: "take-1".to_string(),
                endpoint: endpoint.clone(),
                operation: WorkerOperation::Take,
                payload: Vec::new(),
                arguments: BTreeMap::from([("owner".to_string(), "browser".to_string())]),
            })
            .expect("lock can be taken");
        let release = runtime
            .handle(WorkerRequestCore {
                request_id: "release-1".to_string(),
                endpoint,
                operation: WorkerOperation::Release,
                payload: Vec::new(),
                arguments: BTreeMap::new(),
            })
            .expect("lock can be released");

        assert_eq!(lease.metadata["owner"], "browser");
        assert!(lease.metadata["acquired_time_ns"].parse::<u128>().is_ok());
        assert_eq!(release.metadata["released"], "true");
    }

    #[test]
    fn clock_endpoint_ticks_and_reads_system_time() {
        let mut runtime = RuntimeCore::pubsub("runtime-a".to_string(), 8).expect("valid runtime");
        let endpoint = WorkerEndpointRefCore {
            runtime_id: "runtime-a".to_string(),
            endpoint_id: "default.clock".to_string(),
            kind: WorkerEndpointKind::Clock,
        };

        let first_tick = runtime
            .handle(WorkerRequestCore {
                request_id: "tick-1".to_string(),
                endpoint: endpoint.clone(),
                operation: WorkerOperation::Tick,
                payload: Vec::new(),
                arguments: BTreeMap::new(),
            })
            .expect("clock can tick");
        let second_tick = runtime
            .handle(WorkerRequestCore {
                request_id: "tick-2".to_string(),
                endpoint: endpoint.clone(),
                operation: WorkerOperation::Tick,
                payload: Vec::new(),
                arguments: BTreeMap::new(),
            })
            .expect("clock can tick again");
        let now = runtime
            .handle(WorkerRequestCore {
                request_id: "now-1".to_string(),
                endpoint,
                operation: WorkerOperation::Now,
                payload: Vec::new(),
                arguments: BTreeMap::new(),
            })
            .expect("clock can read time");

        assert_eq!(first_tick.metadata["tick"], "1");
        assert_eq!(second_tick.metadata["tick"], "2");
        assert!(now.metadata["now_ns"].parse::<u128>().is_ok());
    }
}
