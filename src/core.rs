use std::collections::{HashMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Plane {
    Read,
    Write,
    State,
    Query,
    Debug,
}

impl Plane {
    pub fn as_str(self) -> &'static str {
        match self {
            Plane::Read => "read",
            Plane::Write => "write",
            Plane::State => "state",
            Plane::Query => "query",
            Plane::Debug => "debug",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Layer {
    Raw,
    Logical,
    Shadow,
    Bulk,
    Internal,
    Ephemeral,
}

impl Layer {
    pub fn as_str(self) -> &'static str {
        match self {
            Layer::Raw => "raw",
            Layer::Logical => "logical",
            Layer::Shadow => "shadow",
            Layer::Bulk => "bulk",
            Layer::Internal => "internal",
            Layer::Ephemeral => "ephemeral",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Variant {
    Meta,
    Payload,
    Request,
    Desired,
    Reported,
    Effective,
    Ack,
    State,
    QueryRequest,
    QueryResponse,
    Event,
    Health,
}

impl Variant {
    pub fn as_str(self) -> &'static str {
        match self {
            Variant::Meta => "meta",
            Variant::Payload => "payload",
            Variant::Request => "request",
            Variant::Desired => "desired",
            Variant::Reported => "reported",
            Variant::Effective => "effective",
            Variant::Ack => "ack",
            Variant::State => "state",
            Variant::QueryRequest => "query_request",
            Variant::QueryResponse => "query_response",
            Variant::Event => "event",
            Variant::Health => "health",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ProducerKind {
    Device,
    FirmwareAgent,
    Transform,
    ControlLoop,
    Mailbox,
    QueryService,
    Application,
    Bridge,
    Reconciler,
    LifecycleService,
}

impl ProducerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ProducerKind::Device => "device",
            ProducerKind::FirmwareAgent => "firmware_agent",
            ProducerKind::Transform => "transform",
            ProducerKind::ControlLoop => "control_loop",
            ProducerKind::Mailbox => "mailbox",
            ProducerKind::QueryService => "query_service",
            ProducerKind::Application => "application",
            ProducerKind::Bridge => "bridge",
            ProducerKind::Reconciler => "reconciler",
            ProducerKind::LifecycleService => "lifecycle_service",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum TaintDomain {
    Time,
    Order,
    Delivery,
    Determinism,
    Scheduling,
    Trust,
    Coherence,
}

impl TaintDomain {
    pub fn as_str(self) -> &'static str {
        match self {
            TaintDomain::Time => "time",
            TaintDomain::Order => "order",
            TaintDomain::Delivery => "delivery",
            TaintDomain::Determinism => "determinism",
            TaintDomain::Scheduling => "scheduling",
            TaintDomain::Trust => "trust",
            TaintDomain::Coherence => "coherence",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum DeliveryMode {
    MpscSerial,
    MpmcUnique,
    MpmcReplicated,
    KeyAffine,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum OrderingPolicy {
    Fifo,
    PriorityStable,
    WeightedFair,
    RoundRobinByProducer,
    KeyedFifo,
    LatestOnly,
    Unordered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum OverflowPolicy {
    Block,
    DropOldest,
    DropNewest,
    CoalesceLatest,
    DeadlineDrop,
    SpillToStore,
    RejectWrite,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct NamespaceRefCore {
    pub plane: Plane,
    pub layer: Layer,
    pub owner: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SchemaRefCore {
    pub schema_id: String,
    pub version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RouteRefCore {
    pub namespace: NamespaceRefCore,
    pub family: String,
    pub stream: String,
    pub variant: Variant,
    pub schema: SchemaRefCore,
}

impl RouteRefCore {
    pub fn display(&self) -> String {
        format!(
            "{}.{}.{}.{}.{}.v{}",
            self.namespace.plane.as_str(),
            self.namespace.layer.as_str(),
            self.namespace.owner,
            self.family,
            self.stream,
            self.schema.version
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProducerRefCore {
    pub producer_id: String,
    pub kind: ProducerKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeRefCore {
    pub runtime_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClockDomainRefCore {
    pub clock_domain_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadRefCore {
    pub payload_id: String,
    pub logical_length_bytes: u64,
    pub codec_id: String,
    pub inline_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaintMarkCore {
    pub domain: TaintDomain,
    pub value_id: String,
    pub origin_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleConditionCore {
    NotBeforeEpoch(u64),
    WaitForAckRoute(RouteRefCore),
    CreditClass(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleGuardCore {
    pub condition: ScheduleConditionCore,
    pub expires_at_epoch: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClosedEnvelopeCore {
    pub route: RouteRefCore,
    pub producer: ProducerRefCore,
    pub emitter: RuntimeRefCore,
    pub seq_source: u64,
    pub control_epoch: Option<u64>,
    pub taints: Vec<TaintMarkCore>,
    pub guards: Vec<ScheduleGuardCore>,
    pub payload_ref: PayloadRefCore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenedEnvelopeCore {
    pub closed: ClosedEnvelopeCore,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityBlockCore {
    pub route_ref: RouteRefCore,
    pub namespace_ref: NamespaceRefCore,
    pub producer_ref: ProducerRefCore,
    pub owning_runtime_kind: String,
    pub stream_family: String,
    pub stream_variant: Variant,
    pub aliases: Vec<String>,
    pub human_description: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaBlockCore {
    pub schema_ref: SchemaRefCore,
    pub payload_kind: String,
    pub codec_ref: String,
    pub structured_payload_type: String,
    pub payload_open_policy: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimeBlockCore {
    pub clock_domain: ClockDomainRefCore,
    pub event_time_policy: String,
    pub processing_time_allowed: bool,
    pub watermark_policy: String,
    pub control_epoch_policy: String,
    pub ttl_policy: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderingBlockCore {
    pub partition_spec: String,
    pub sequence_source_kind: String,
    pub resequence_policy: String,
    pub dedupe_policy: String,
    pub causality_policy: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowBlockCore {
    pub backpressure_policy: String,
    pub credit_class: String,
    pub mailbox_policy: String,
    pub async_boundary_kind: String,
    pub overflow_policy: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionBlockCore {
    pub latest_replay_policy: String,
    pub durability_class: String,
    pub replay_window: String,
    pub payload_retention_policy: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecurityBlockCore {
    pub read_capabilities: Vec<String>,
    pub write_capabilities: Vec<String>,
    pub payload_open_capabilities: Vec<String>,
    pub redaction_policy: String,
    pub integrity_policy: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibilityBlockCore {
    pub private_or_exported: String,
    pub third_party_subscription_allowed: bool,
    pub query_plane_visibility: String,
    pub debug_plane_visibility: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentBlockCore {
    pub locality: String,
    pub transport_preferences: Vec<String>,
    pub device_class: String,
    pub resource_class: String,
    pub ephemeral_scope: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DebugBlockCore {
    pub audit_enabled: bool,
    pub trace_enabled: bool,
    pub metrics_enabled: bool,
    pub payload_peek_allowed: bool,
    pub explain_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortDescriptorCore {
    pub identity: IdentityBlockCore,
    pub schema: SchemaBlockCore,
    pub time: TimeBlockCore,
    pub ordering: OrderingBlockCore,
    pub flow: FlowBlockCore,
    pub retention: RetentionBlockCore,
    pub security: SecurityBlockCore,
    pub visibility: VisibilityBlockCore,
    pub environment: EnvironmentBlockCore,
    pub debug: DebugBlockCore,
}

impl PortDescriptorCore {
    pub fn for_route(route: &RouteRefCore) -> Self {
        Self {
            identity: IdentityBlockCore {
                route_ref: route.clone(),
                namespace_ref: route.namespace.clone(),
                producer_ref: ProducerRefCore {
                    producer_id: route.namespace.owner.clone(),
                    kind: ProducerKind::Application,
                },
                owning_runtime_kind: "in_memory".to_string(),
                stream_family: route.family.clone(),
                stream_variant: route.variant,
                aliases: vec![route.display()],
                human_description: format!("Manyfold port for {}", route.display()),
            },
            schema: SchemaBlockCore {
                schema_ref: route.schema.clone(),
                payload_kind: "structured".to_string(),
                codec_ref: "identity".to_string(),
                structured_payload_type: route.schema.schema_id.clone(),
                payload_open_policy: "lazy".to_string(),
            },
            time: TimeBlockCore {
                clock_domain: ClockDomainRefCore {
                    clock_domain_id: "monotonic".to_string(),
                },
                event_time_policy: "ingest".to_string(),
                processing_time_allowed: true,
                watermark_policy: "none".to_string(),
                control_epoch_policy: "optional".to_string(),
                ttl_policy: "retain_latest".to_string(),
            },
            ordering: OrderingBlockCore {
                partition_spec: "unpartitioned".to_string(),
                sequence_source_kind: "route_local".to_string(),
                resequence_policy: "none".to_string(),
                dedupe_policy: "none".to_string(),
                causality_policy: "opaque".to_string(),
            },
            flow: FlowBlockCore {
                backpressure_policy: "propagate".to_string(),
                credit_class: "default".to_string(),
                mailbox_policy: "none".to_string(),
                async_boundary_kind: "inline".to_string(),
                overflow_policy: "reject_write".to_string(),
            },
            retention: RetentionBlockCore {
                latest_replay_policy: "latest_only".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "none".to_string(),
                payload_retention_policy: "inline".to_string(),
            },
            security: SecurityBlockCore {
                read_capabilities: vec!["read".to_string()],
                write_capabilities: vec!["write".to_string()],
                payload_open_capabilities: vec!["payload_open".to_string()],
                redaction_policy: "none".to_string(),
                integrity_policy: "best_effort".to_string(),
            },
            visibility: VisibilityBlockCore {
                private_or_exported: "private".to_string(),
                third_party_subscription_allowed: false,
                query_plane_visibility: "owner".to_string(),
                debug_plane_visibility: "owner".to_string(),
            },
            environment: EnvironmentBlockCore {
                locality: "process".to_string(),
                transport_preferences: vec!["memory".to_string()],
                device_class: "generic".to_string(),
                resource_class: "standard".to_string(),
                ephemeral_scope: None,
            },
            debug: DebugBlockCore {
                audit_enabled: true,
                trace_enabled: true,
                metrics_enabled: true,
                payload_peek_allowed: true,
                explain_enabled: true,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteBindingCore {
    pub request: RouteRefCore,
    pub desired: RouteRefCore,
    pub reported: RouteRefCore,
    pub effective: RouteRefCore,
    pub ack: Option<RouteRefCore>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MailboxDescriptorCore {
    pub delivery_mode: DeliveryMode,
    pub ordering_policy: OrderingPolicy,
    pub overflow_policy: OverflowPolicy,
    pub capacity: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MailboxCore {
    pub ingress: RouteRefCore,
    pub egress: RouteRefCore,
    pub descriptor: MailboxDescriptorCore,
    pub queue: VecDeque<ClosedEnvelopeCore>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlLoopCore {
    pub name: String,
    pub read_routes: Vec<RouteRefCore>,
    pub write_route: RouteRefCore,
    pub epoch: u64,
}

impl ControlLoopCore {
    pub fn tick(&mut self) -> ClosedEnvelopeCore {
        self.epoch += 1;
        ClosedEnvelopeCore {
            route: self.write_route.clone(),
            producer: ProducerRefCore {
                producer_id: self.name.clone(),
                kind: ProducerKind::ControlLoop,
            },
            emitter: RuntimeRefCore {
                runtime_id: "runtime:in_memory".to_string(),
            },
            seq_source: self.epoch,
            control_epoch: Some(self.epoch),
            taints: vec![TaintMarkCore {
                domain: TaintDomain::Scheduling,
                value_id: "SCHED_GATED".to_string(),
                origin_id: self.name.clone(),
            }],
            guards: vec![ScheduleGuardCore {
                condition: ScheduleConditionCore::NotBeforeEpoch(self.epoch + 1),
                expires_at_epoch: None,
            }],
            payload_ref: PayloadRefCore {
                payload_id: format!("control-loop-{}", self.epoch),
                logical_length_bytes: 0,
                codec_id: "identity".to_string(),
                inline_bytes: Vec::new(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryKindCore {
    Catalog,
    DescribeRoute(RouteRefCore),
    Latest(RouteRefCore),
    Topology,
    Trace,
    Replay(RouteRefCore),
    ValidateGraph,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryResultCore {
    Catalog(Vec<RouteRefCore>),
    DescribeRoute(PortDescriptorCore),
    Latest(Option<ClosedEnvelopeCore>),
    Topology(Vec<(String, String)>),
    Trace(Vec<ClosedEnvelopeCore>),
    Replay(Vec<ClosedEnvelopeCore>),
    ValidateGraph(Vec<String>),
}

#[derive(Default)]
pub struct GraphCore {
    pub descriptors: HashMap<RouteRefCore, PortDescriptorCore>,
    pub latest: HashMap<RouteRefCore, ClosedEnvelopeCore>,
    pub edges: Vec<(RouteRefCore, RouteRefCore)>,
    pub bindings: HashMap<String, WriteBindingCore>,
    pub mailboxes: HashMap<String, MailboxCore>,
    pub loops: HashMap<String, ControlLoopCore>,
}

impl GraphCore {
    pub fn register_port(&mut self, route: RouteRefCore) -> RouteRefCore {
        self.descriptors
            .entry(route.clone())
            .or_insert_with(|| PortDescriptorCore::for_route(&route));
        route
    }

    pub fn register_binding(&mut self, name: String, binding: WriteBindingCore) {
        for route in [
            binding.request.clone(),
            binding.desired.clone(),
            binding.reported.clone(),
            binding.effective.clone(),
        ] {
            self.register_port(route);
        }
        if let Some(ack) = binding.ack.clone() {
            self.register_port(ack);
        }
        self.bindings.insert(name, binding);
    }

    pub fn register_mailbox(&mut self, name: String, mailbox: MailboxCore) {
        self.register_port(mailbox.ingress.clone());
        self.register_port(mailbox.egress.clone());
        self.mailboxes.insert(name, mailbox);
    }

    pub fn connect(&mut self, source: &RouteRefCore, sink: &RouteRefCore) {
        self.register_port(source.clone());
        self.register_port(sink.clone());
        self.edges.push((source.clone(), sink.clone()));
    }

    pub fn write(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> ClosedEnvelopeCore {
        self.register_port(route.clone());
        let seq_source = self
            .latest
            .get(route)
            .map(|envelope| envelope.seq_source + 1)
            .unwrap_or(1);
        let envelope = ClosedEnvelopeCore {
            route: route.clone(),
            producer,
            emitter: RuntimeRefCore {
                runtime_id: "runtime:in_memory".to_string(),
            },
            seq_source,
            control_epoch,
            taints: Vec::new(),
            guards: Vec::new(),
            payload_ref: PayloadRefCore {
                payload_id: format!("{}:{}", route.display(), seq_source),
                logical_length_bytes: payload.len() as u64,
                codec_id: "identity".to_string(),
                inline_bytes: payload,
            },
        };
        self.latest.insert(route.clone(), envelope.clone());
        envelope
    }

    pub fn query(&self, query: QueryKindCore) -> QueryResultCore {
        match query {
            QueryKindCore::Catalog => {
                let mut routes = self.descriptors.keys().cloned().collect::<Vec<_>>();
                routes.sort_by_key(RouteRefCore::display);
                QueryResultCore::Catalog(routes)
            }
            QueryKindCore::DescribeRoute(route) => QueryResultCore::DescribeRoute(
                self.descriptors
                    .get(&route)
                    .cloned()
                    .unwrap_or_else(|| PortDescriptorCore::for_route(&route)),
            ),
            QueryKindCore::Latest(route) => QueryResultCore::Latest(self.latest.get(&route).cloned()),
            QueryKindCore::Topology => QueryResultCore::Topology(
                self.edges
                    .iter()
                    .map(|(left, right)| (left.display(), right.display()))
                    .collect(),
            ),
            QueryKindCore::Trace => {
                let mut items = self.latest.values().cloned().collect::<Vec<_>>();
                items.sort_by_key(|item| item.seq_source);
                QueryResultCore::Trace(items)
            }
            QueryKindCore::Replay(route) => QueryResultCore::Replay(
                self.latest
                    .get(&route)
                    .cloned()
                    .into_iter()
                    .collect::<Vec<_>>(),
            ),
            QueryKindCore::ValidateGraph => {
                let mut issues = Vec::new();
                for (source, sink) in &self.edges {
                    if source.schema != sink.schema {
                        issues.push(format!(
                            "Schema mismatch: {} -> {}",
                            source.display(),
                            sink.display()
                        ));
                    }
                }
                QueryResultCore::ValidateGraph(issues)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_route(plane: Plane, layer: Layer, owner: &str, family: &str, stream: &str, variant: Variant) -> RouteRefCore {
        RouteRefCore {
            namespace: NamespaceRefCore {
                plane,
                layer,
                owner: owner.to_string(),
            },
            family: family.to_string(),
            stream: stream.to_string(),
            variant,
            schema: SchemaRefCore {
                schema_id: "Sample".to_string(),
                version: 1,
            },
        }
    }

    #[test]
    fn write_tracks_latest_envelope() {
        let route = sample_route(Plane::Read, Layer::Logical, "app", "imu", "accel", Variant::Meta);
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "app".to_string(),
            kind: ProducerKind::Application,
        };

        let first = graph.write(&route, b"one".to_vec(), producer.clone(), None);
        let second = graph.write(&route, b"two".to_vec(), producer, None);

        assert_eq!(first.seq_source, 1);
        assert_eq!(second.seq_source, 2);
        assert_eq!(
            graph.query(QueryKindCore::Latest(route.clone())),
            QueryResultCore::Latest(Some(second))
        );
    }

    #[test]
    fn control_loop_emits_guarded_epoch_write() {
        let write_route = sample_route(
            Plane::Write,
            Layer::Logical,
            "motor",
            "speed",
            "pid",
            Variant::Request,
        );
        let mut loop_core = ControlLoopCore {
            name: "SpeedPid".to_string(),
            read_routes: Vec::new(),
            write_route,
            epoch: 0,
        };

        let emitted = loop_core.tick();

        assert_eq!(emitted.control_epoch, Some(1));
        assert_eq!(emitted.guards.len(), 1);
        assert_eq!(
            emitted.guards[0].condition,
            ScheduleConditionCore::NotBeforeEpoch(2)
        );
    }

    #[test]
    fn validate_graph_reports_schema_mismatch() {
        let left = sample_route(Plane::Read, Layer::Raw, "imu", "imu", "accel", Variant::Meta);
        let mut right = sample_route(
            Plane::Read,
            Layer::Logical,
            "pose",
            "pose",
            "estimate",
            Variant::Meta,
        );
        right.schema.schema_id = "Pose".to_string();

        let mut graph = GraphCore::default();
        graph.connect(&left, &right);

        let QueryResultCore::ValidateGraph(issues) = graph.query(QueryKindCore::ValidateGraph) else {
            panic!("unexpected query result");
        };
        assert_eq!(issues.len(), 1);
    }
}
