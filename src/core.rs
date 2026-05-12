use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

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
            "{}.{}.{}.{}.{}.{}.v{}",
            self.namespace.plane.as_str(),
            self.namespace.layer.as_str(),
            self.namespace.owner,
            self.family,
            self.stream,
            self.variant.as_str(),
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
    pub link_seq: Option<u64>,
    pub device_time_unix_ms: Option<u64>,
    pub ingest_time_unix_ms: u64,
    pub logical_time_unix_ms: u64,
    pub logical_clock_domain: ClockDomainRefCore,
    pub partition_key: Option<String>,
    pub causality_id: Option<String>,
    pub correlation_id: Option<String>,
    pub trace_id: Option<String>,
    pub control_epoch: Option<u64>,
    pub taints: Vec<TaintMarkCore>,
    pub guards: Vec<ScheduleGuardCore>,
    pub payload_ref: PayloadRefCore,
    pub qos_class: String,
    pub security_labels: Vec<String>,
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
    fn default_clock_domain(route: &RouteRefCore) -> ClockDomainRefCore {
        // Descriptor defaults are derived from namespace semantics so callers do
        // not need to hand-specify the common RFC policy buckets.
        let clock_domain_id = match (route.namespace.plane, route.namespace.layer) {
            (_, Layer::Ephemeral) => "ephemeral",
            (Plane::Write, _) => "control_epoch",
            _ => "monotonic",
        };
        ClockDomainRefCore {
            clock_domain_id: clock_domain_id.to_string(),
        }
    }

    fn default_credit_class(route: &RouteRefCore) -> &'static str {
        match (route.namespace.plane, route.namespace.layer) {
            (_, Layer::Bulk) => "bulk_payload",
            (Plane::Write, _) => "control",
            _ => "default",
        }
    }

    fn default_payload_open_policy(route: &RouteRefCore) -> &'static str {
        match route.namespace.layer {
            Layer::Bulk => "lazy_external",
            Layer::Internal => "owner_only",
            _ => "lazy",
        }
    }

    fn default_payload_retention(route: &RouteRefCore) -> &'static str {
        match route.namespace.layer {
            Layer::Bulk => "external_store",
            Layer::Ephemeral => "non_replayable",
            _ => "separate_store",
        }
    }

    fn default_visibility(
        route: &RouteRefCore,
    ) -> (&'static str, bool, &'static str, &'static str) {
        match route.namespace.layer {
            Layer::Ephemeral | Layer::Internal => ("private", false, "owner", "owner"),
            _ => ("private", false, "owner", "owner"),
        }
    }

    fn default_locality(route: &RouteRefCore) -> (&'static str, Vec<String>, Option<String>) {
        match route.namespace.layer {
            Layer::Bulk => (
                "process",
                vec!["memory".to_string(), "bulk_link".to_string()],
                None,
            ),
            Layer::Ephemeral => (
                "process",
                vec!["memory".to_string()],
                Some(route.namespace.owner.clone()),
            ),
            _ => ("process", vec!["memory".to_string()], None),
        }
    }

    fn producer_kind(route: &RouteRefCore) -> ProducerKind {
        match (route.namespace.plane, route.namespace.layer) {
            (_, Layer::Internal) => ProducerKind::Mailbox,
            (Plane::Query, _) | (Plane::Debug, _) => ProducerKind::QueryService,
            (Plane::Write, Layer::Shadow) => ProducerKind::Reconciler,
            (Plane::Write, _) => ProducerKind::Application,
            (Plane::Read, Layer::Raw) => ProducerKind::Device,
            _ => ProducerKind::Application,
        }
    }

    pub fn for_route(route: &RouteRefCore) -> Self {
        let visibility = Self::default_visibility(route);
        let locality = Self::default_locality(route);
        let route_display = route.display();
        Self {
            identity: IdentityBlockCore {
                route_ref: route.clone(),
                namespace_ref: route.namespace.clone(),
                producer_ref: ProducerRefCore {
                    producer_id: route.namespace.owner.clone(),
                    kind: Self::producer_kind(route),
                },
                owning_runtime_kind: "in_memory".to_string(),
                stream_family: route.family.clone(),
                stream_variant: route.variant,
                aliases: vec![route_display.clone()],
                human_description: format!("Manyfold port for {route_display}"),
            },
            schema: SchemaBlockCore {
                schema_ref: route.schema.clone(),
                payload_kind: "structured".to_string(),
                codec_ref: "identity".to_string(),
                structured_payload_type: route.schema.schema_id.clone(),
                payload_open_policy: Self::default_payload_open_policy(route).to_string(),
            },
            time: TimeBlockCore {
                clock_domain: Self::default_clock_domain(route),
                event_time_policy: if route.namespace.plane == Plane::Write {
                    "control_epoch_or_ingest".to_string()
                } else {
                    "ingest".to_string()
                },
                processing_time_allowed: true,
                watermark_policy: if route.namespace.layer == Layer::Bulk {
                    "recommended".to_string()
                } else {
                    "none".to_string()
                },
                control_epoch_policy: if route.namespace.plane == Plane::Write {
                    "allowed".to_string()
                } else {
                    "optional".to_string()
                },
                ttl_policy: if route.namespace.layer == Layer::Ephemeral {
                    "ttl_required".to_string()
                } else {
                    "retain_latest".to_string()
                },
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
                credit_class: Self::default_credit_class(route).to_string(),
                mailbox_policy: "none".to_string(),
                async_boundary_kind: "inline".to_string(),
                overflow_policy: "reject_write".to_string(),
            },
            retention: RetentionBlockCore {
                latest_replay_policy: if route.namespace.layer == Layer::Ephemeral {
                    "none".to_string()
                } else {
                    "latest_only".to_string()
                },
                durability_class: "memory".to_string(),
                replay_window: "none".to_string(),
                payload_retention_policy: Self::default_payload_retention(route).to_string(),
            },
            security: SecurityBlockCore {
                read_capabilities: vec!["read".to_string()],
                write_capabilities: vec!["write".to_string()],
                payload_open_capabilities: vec!["payload_open".to_string()],
                redaction_policy: "none".to_string(),
                integrity_policy: "best_effort".to_string(),
            },
            visibility: VisibilityBlockCore {
                private_or_exported: visibility.0.to_string(),
                third_party_subscription_allowed: visibility.1,
                query_plane_visibility: visibility.2.to_string(),
                debug_plane_visibility: visibility.3.to_string(),
            },
            environment: EnvironmentBlockCore {
                locality: locality.0.to_string(),
                transport_preferences: locality.1,
                device_class: "generic".to_string(),
                resource_class: "standard".to_string(),
                ephemeral_scope: locality.2,
            },
            debug: DebugBlockCore {
                audit_enabled: true,
                trace_enabled: true,
                metrics_enabled: true,
                payload_peek_allowed: route.namespace.layer != Layer::Bulk,
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

impl WriteBindingCore {
    pub fn validate(&self) -> Result<(), String> {
        if self.request.namespace.plane != Plane::Write || self.request.variant != Variant::Request
        {
            return Err(
                "request route must live under the write plane with Variant::Request".to_string(),
            );
        }
        if self.request.namespace.layer == Layer::Shadow {
            return Err("request route must not use the shadow layer".to_string());
        }
        for (name, route, expected_variant) in [
            ("desired", &self.desired, Variant::Desired),
            ("reported", &self.reported, Variant::Reported),
            ("effective", &self.effective, Variant::Effective),
        ] {
            if route.namespace.plane != Plane::Write || route.namespace.layer != Layer::Shadow {
                return Err(format!("{name} route must live under write.shadow"));
            }
            if route.variant != expected_variant {
                return Err(format!(
                    "{name} route must use variant {}",
                    expected_variant.as_str()
                ));
            }
            if route.namespace.owner != self.request.namespace.owner
                || route.family != self.request.family
                || route.stream != self.request.stream
            {
                return Err(format!(
                    "{name} route must share owner/family/stream with the request route"
                ));
            }
        }
        if self.desired.schema != self.request.schema
            || self.effective.schema != self.request.schema
        {
            return Err("desired and effective routes must share the request schema".to_string());
        }
        if let Some(ack) = &self.ack {
            if ack.namespace.plane != Plane::Write || ack.namespace.layer != Layer::Shadow {
                return Err("ack route must live under write.shadow".to_string());
            }
            if ack.variant != Variant::Ack {
                return Err("ack route must use Variant::Ack".to_string());
            }
        }
        Ok(())
    }
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
    pub blocked_writes: u64,
    pub dropped_messages: u64,
    pub coalesced_messages: u64,
    pub delivered_messages: u64,
    pub largest_queue_depth: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreditSnapshotCore {
    pub route_display: String,
    pub credit_class: String,
    pub available: u64,
    pub blocked_senders: u64,
    pub dropped_messages: u64,
    pub largest_queue_depth: usize,
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
        let now = GraphCore::now_unix_ms();
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
            link_seq: None,
            device_time_unix_ms: None,
            ingest_time_unix_ms: now,
            logical_time_unix_ms: now,
            logical_clock_domain: ClockDomainRefCore {
                clock_domain_id: "control_epoch".to_string(),
            },
            partition_key: None,
            causality_id: Some(format!("{}#{}", self.name, self.epoch)),
            correlation_id: Some(format!("control-epoch-{}", self.epoch)),
            trace_id: Some(format!("trace:{}:{}", self.name, self.epoch)),
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
            qos_class: "control".to_string(),
            security_labels: vec!["write".to_string()],
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditEventCore {
    pub route: RouteRefCore,
    pub action: String,
    pub payload_id: Option<String>,
    pub actor_id: String,
    pub seq_source: Option<u64>,
}

#[derive(Default)]
pub struct GraphCore {
    pub descriptors: HashMap<RouteRefCore, PortDescriptorCore>,
    pub latest: HashMap<RouteRefCore, ClosedEnvelopeCore>,
    pub history: HashMap<RouteRefCore, Vec<ClosedEnvelopeCore>>,
    pub payloads: HashMap<String, Vec<u8>>,
    pub route_audit: HashMap<RouteRefCore, Vec<AuditEventCore>>,
    pub edges: Vec<(RouteRefCore, RouteRefCore)>,
    pub bindings: HashMap<String, WriteBindingCore>,
    pub request_bindings: HashMap<RouteRefCore, WriteBindingCore>,
    pub mailboxes: BTreeMap<String, MailboxCore>,
    pub loops: HashMap<String, ControlLoopCore>,
}

impl GraphCore {
    fn now_unix_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn validate_route(route: &RouteRefCore) -> Result<(), String> {
        let valid = match route.namespace.plane {
            Plane::Read => matches!(
                route.variant,
                Variant::Meta
                    | Variant::Payload
                    | Variant::State
                    | Variant::Event
                    | Variant::Health
            ),
            Plane::Write => matches!(
                route.variant,
                Variant::Request
                    | Variant::Desired
                    | Variant::Reported
                    | Variant::Effective
                    | Variant::Ack
            ),
            Plane::State => route.variant == Variant::State,
            Plane::Query => matches!(
                route.variant,
                Variant::QueryRequest | Variant::QueryResponse
            ),
            Plane::Debug => matches!(
                route.variant,
                Variant::Meta | Variant::Event | Variant::Health
            ),
        };
        if !valid {
            return Err(format!(
                "variant {} is not valid for plane {}",
                route.variant.as_str(),
                route.namespace.plane.as_str()
            ));
        }
        if route.namespace.plane != Plane::Write && route.namespace.layer == Layer::Shadow {
            return Err("shadow routes are only valid under the write plane".to_string());
        }
        if route.namespace.plane == Plane::Write
            && route.variant == Variant::Request
            && route.namespace.layer == Layer::Shadow
        {
            return Err("write request routes must not use the shadow layer".to_string());
        }
        Ok(())
    }

    fn append_audit(
        &mut self,
        route: &RouteRefCore,
        action: &str,
        actor_id: &str,
        payload_id: Option<String>,
        seq_source: Option<u64>,
    ) {
        self.route_audit
            .entry(route.clone())
            .or_default()
            .push(AuditEventCore {
                route: route.clone(),
                action: action.to_string(),
                payload_id,
                actor_id: actor_id.to_string(),
                seq_source,
            });
    }

    fn event_taints(route: &RouteRefCore, control_epoch: Option<u64>) -> Vec<TaintMarkCore> {
        let mut taints = Vec::new();
        if route.namespace.plane == Plane::Write && route.variant == Variant::Request {
            taints.push(TaintMarkCore {
                domain: TaintDomain::Coherence,
                value_id: "COHERENCE_WRITE_PENDING".to_string(),
                origin_id: route.display(),
            });
        }
        if route.namespace.layer == Layer::Ephemeral {
            taints.push(TaintMarkCore {
                domain: TaintDomain::Determinism,
                value_id: "DET_NONREPLAYABLE".to_string(),
                origin_id: route.display(),
            });
        }
        if control_epoch.is_some() {
            taints.push(TaintMarkCore {
                domain: TaintDomain::Scheduling,
                value_id: "SCHED_READY".to_string(),
                origin_id: route.display(),
            });
        }
        taints
    }

    fn payload_bytes_for(&self, envelope: &ClosedEnvelopeCore) -> Vec<u8> {
        self.payloads
            .get(&envelope.payload_ref.payload_id)
            .cloned()
            .unwrap_or_else(|| envelope.payload_ref.inline_bytes.clone())
    }

    pub fn register_port(&mut self, route: RouteRefCore) -> RouteRefCore {
        if let Err(message) = Self::validate_route(&route) {
            panic!("invalid route registered: {message}");
        }
        self.descriptors
            .entry(route.clone())
            .or_insert_with(|| PortDescriptorCore::for_route(&route));
        route
    }

    pub fn register_binding(&mut self, name: String, binding: WriteBindingCore) {
        if let Err(message) = binding.validate() {
            panic!("invalid write binding: {message}");
        }
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
        self.request_bindings
            .insert(binding.request.clone(), binding.clone());
        self.bindings.insert(name, binding);
    }

    pub fn register_mailbox(&mut self, name: String, mailbox: MailboxCore) {
        self.register_port(mailbox.ingress.clone());
        self.register_port(mailbox.egress.clone());
        self.mailboxes.insert(name, mailbox);
    }

    pub fn connect(&mut self, source: &RouteRefCore, sink: &RouteRefCore) -> bool {
        self.register_port(source.clone());
        self.register_port(sink.clone());
        let edge = (source.clone(), sink.clone());
        if self.edges.contains(&edge) {
            return false;
        }
        self.edges.push(edge);
        self.try_drain_mailbox_for_source(source);
        true
    }

    pub fn disconnect(&mut self, source: &RouteRefCore, sink: &RouteRefCore) -> bool {
        let Some(index) = self
            .edges
            .iter()
            .position(|(left, right)| left == source && right == sink)
        else {
            return false;
        };
        self.edges.remove(index);
        true
    }

    fn direct_write(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> ClosedEnvelopeCore {
        self.register_port(route.clone());
        let descriptor = self
            .descriptors
            .get(route)
            .cloned()
            .unwrap_or_else(|| PortDescriptorCore::for_route(route));
        let seq_source = self
            .latest
            .get(route)
            .map(|envelope| envelope.seq_source + 1)
            .unwrap_or(1);
        let route_display = route.display();
        let payload_id = format!("{route_display}:{seq_source}");
        let payload_len = payload.len() as u64;
        let now = Self::now_unix_ms();
        self.payloads.insert(payload_id.clone(), payload);
        let envelope = ClosedEnvelopeCore {
            route: route.clone(),
            producer: producer.clone(),
            emitter: RuntimeRefCore {
                runtime_id: "runtime:in_memory".to_string(),
            },
            seq_source,
            link_seq: None,
            device_time_unix_ms: None,
            ingest_time_unix_ms: now,
            logical_time_unix_ms: now,
            logical_clock_domain: descriptor.time.clock_domain.clone(),
            partition_key: None,
            causality_id: Some(format!("{route_display}#{seq_source}")),
            correlation_id: control_epoch.map(|epoch| format!("control-epoch-{epoch}")),
            trace_id: Some(format!("trace:{route_display}:{seq_source}")),
            control_epoch,
            taints: Self::event_taints(route, control_epoch),
            guards: Vec::new(),
            payload_ref: PayloadRefCore {
                payload_id: payload_id.clone(),
                logical_length_bytes: payload_len,
                codec_id: "identity".to_string(),
                inline_bytes: Vec::new(),
            },
            qos_class: descriptor.flow.credit_class.clone(),
            security_labels: descriptor.security.read_capabilities.clone(),
        };
        self.latest.insert(route.clone(), envelope.clone());
        self.history
            .entry(route.clone())
            .or_default()
            .push(envelope.clone());
        self.append_audit(
            route,
            "write",
            &producer.producer_id,
            Some(payload_id),
            Some(seq_source),
        );
        envelope
    }

    fn route_credit_snapshot(&self, route: &RouteRefCore) -> CreditSnapshotCore {
        let descriptor = self
            .descriptors
            .get(route)
            .cloned()
            .unwrap_or_else(|| PortDescriptorCore::for_route(route));
        let maybe_mailbox = self
            .mailboxes
            .values()
            .find(|mailbox| mailbox.ingress == *route || mailbox.egress == *route);

        match maybe_mailbox {
            Some(mailbox) => CreditSnapshotCore {
                route_display: route.display(),
                credit_class: descriptor.flow.credit_class,
                available: mailbox
                    .descriptor
                    .capacity
                    .saturating_sub(mailbox.queue.len()) as u64,
                blocked_senders: mailbox.blocked_writes,
                dropped_messages: mailbox.dropped_messages,
                largest_queue_depth: mailbox.largest_queue_depth,
            },
            None => CreditSnapshotCore {
                route_display: route.display(),
                credit_class: descriptor.flow.credit_class,
                available: u64::MAX,
                blocked_senders: 0,
                dropped_messages: 0,
                largest_queue_depth: self.history.get(route).map_or(0, Vec::len),
            },
        }
    }

    fn try_enqueue_mailbox(
        &mut self,
        route: &RouteRefCore,
        envelope: ClosedEnvelopeCore,
    ) -> Option<RouteRefCore> {
        // Mailbox ingress is the explicit async boundary. Once a route resolves to
        // mailbox ingress, delivery semantics are owned by the mailbox descriptor
        // rather than by normal edge fanout.
        let Some(mailbox) = self
            .mailboxes
            .values_mut()
            .find(|mailbox| mailbox.ingress == *route)
        else {
            return None;
        };
        let capacity = mailbox.descriptor.capacity;
        let full = mailbox.queue.len() >= capacity;

        if full {
            match mailbox.descriptor.overflow_policy {
                OverflowPolicy::Block | OverflowPolicy::RejectWrite => {
                    mailbox.blocked_writes += 1;
                    return Some(mailbox.egress.clone());
                }
                OverflowPolicy::DropNewest
                | OverflowPolicy::DeadlineDrop
                | OverflowPolicy::SpillToStore => {
                    mailbox.dropped_messages += 1;
                    return Some(mailbox.egress.clone());
                }
                OverflowPolicy::DropOldest => {
                    mailbox.queue.pop_front();
                    mailbox.dropped_messages += 1;
                }
                OverflowPolicy::CoalesceLatest => {
                    if let Some(last) = mailbox.queue.back_mut() {
                        *last = envelope;
                        mailbox.coalesced_messages += 1;
                        return Some(mailbox.egress.clone());
                    }
                }
            }
        }

        mailbox.queue.push_back(envelope);
        mailbox.largest_queue_depth = mailbox.largest_queue_depth.max(mailbox.queue.len());
        Some(mailbox.egress.clone())
    }

    fn try_drain_mailbox_for_source(&mut self, source: &RouteRefCore) -> Vec<ClosedEnvelopeCore> {
        // Draining is demand-shaped by graph topology: if nothing consumes the
        // egress route, items stay queued and consume credit.
        let Some(name) = self
            .mailboxes
            .iter()
            .find(|(_, mailbox)| mailbox.egress == *source)
            .map(|(name, _)| name.clone())
        else {
            return Vec::new();
        };

        let has_consumer = self.edges.iter().any(|(left, _)| left == source);
        if !has_consumer {
            return Vec::new();
        }

        let mut staged = Vec::new();
        if let Some(mailbox) = self.mailboxes.get_mut(&name) {
            while let Some(item) = mailbox.queue.pop_front() {
                mailbox.delivered_messages += 1;
                staged.push((mailbox.egress.clone(), item));
            }
        }

        let mut emitted = Vec::new();
        for item in staged {
            let (egress, queued) = item;
            let payload = self.payload_bytes_for(&queued);
            let forwarded = self.direct_write(
                &egress,
                payload,
                queued.producer.clone(),
                queued.control_epoch,
            );
            emitted.extend(self.route_envelope(forwarded, true));
        }
        emitted
    }

    fn route_envelope(
        &mut self,
        envelope: ClosedEnvelopeCore,
        fanout: bool,
    ) -> Vec<ClosedEnvelopeCore> {
        let route = envelope.route.clone();
        let mut emitted = vec![envelope.clone()];

        if fanout {
            let binding = self.request_bindings.get(&route).cloned();
            let downstream = self
                .edges
                .iter()
                .filter(|(source, _)| *source == route)
                .map(|(_, sink)| sink.clone())
                .collect::<Vec<_>>();

            // A request write projects into shadow state before any downstream fanout
            // so desired-state observability exists even when the device has not yet
            // reported or applied the value.
            if binding.is_some() || !downstream.is_empty() {
                let payload = self.payload_bytes_for(&envelope);
                if let Some(binding) = binding {
                    let desired = self.direct_write(
                        &binding.desired,
                        payload.clone(),
                        ProducerRefCore {
                            producer_id: binding.request.display(),
                            kind: ProducerKind::Reconciler,
                        },
                        envelope.control_epoch,
                    );
                    emitted.push(desired);
                }

                for sink in downstream {
                    let forwarded = self.direct_write(
                        &sink,
                        payload.clone(),
                        envelope.producer.clone(),
                        envelope.control_epoch,
                    );
                    emitted.extend(self.route_envelope(forwarded, false));
                }
            }
        }

        if let Some(egress) = self.try_enqueue_mailbox(&route, envelope.clone()) {
            emitted.extend(self.try_drain_mailbox_for_source(&egress));
        }

        emitted
    }

    pub fn write(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> Vec<ClosedEnvelopeCore> {
        self.register_port(route.clone());
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        self.route_envelope(envelope, true)
    }

    pub fn credit_snapshot(&self) -> Vec<CreditSnapshotCore> {
        let mut snapshots = self
            .descriptors
            .keys()
            .map(|route| self.route_credit_snapshot(route))
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.route_display.cmp(&right.route_display));
        snapshots
    }

    pub fn open_latest(&mut self, route: &RouteRefCore) -> Option<OpenedEnvelopeCore> {
        let closed = self.latest.get(route).cloned()?;
        let payload = self.payloads.get(&closed.payload_ref.payload_id).cloned()?;
        self.append_audit(
            route,
            "payload_open",
            "reader",
            Some(closed.payload_ref.payload_id.clone()),
            Some(closed.seq_source),
        );
        Some(OpenedEnvelopeCore { closed, payload })
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
            QueryKindCore::Latest(route) => {
                QueryResultCore::Latest(self.latest.get(&route).cloned())
            }
            QueryKindCore::Topology => {
                let mut edges = self
                    .edges
                    .iter()
                    .map(|(left, right)| (left.display(), right.display()))
                    .collect::<Vec<_>>();
                edges.sort();
                QueryResultCore::Topology(edges)
            }
            QueryKindCore::Trace => {
                let mut items = self.latest.values().cloned().collect::<Vec<_>>();
                items.sort_by_key(|item| (item.seq_source, item.route.display()));
                QueryResultCore::Trace(items)
            }
            QueryKindCore::Replay(route) => {
                QueryResultCore::Replay(self.history.get(&route).cloned().unwrap_or_default())
            }
            QueryKindCore::ValidateGraph => {
                let mut issues = Vec::new();
                let mut schema_mismatches = self
                    .edges
                    .iter()
                    .filter(|(source, sink)| source.schema != sink.schema)
                    .map(|(source, sink)| {
                        let source_display = source.display();
                        let sink_display = sink.display();
                        (
                            source_display.clone(),
                            sink_display.clone(),
                            format!("Schema mismatch: {source_display} -> {sink_display}"),
                        )
                    })
                    .collect::<Vec<_>>();
                schema_mismatches.sort_by(|left, right| {
                    (left.0.as_str(), left.1.as_str()).cmp(&(right.0.as_str(), right.1.as_str()))
                });
                for (_, _, issue) in schema_mismatches {
                    issues.push(issue);
                }
                for (name, mailbox) in self.mailboxes.iter() {
                    if mailbox.descriptor.capacity == 0 {
                        issues.push(format!("Mailbox {name} must declare positive capacity"));
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

    fn sample_route(
        plane: Plane,
        layer: Layer,
        owner: &str,
        family: &str,
        stream: &str,
        variant: Variant,
    ) -> RouteRefCore {
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
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "app",
            "imu",
            "accel",
            Variant::Meta,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "app".to_string(),
            kind: ProducerKind::Application,
        };

        let first = graph.write(&route, b"one".to_vec(), producer.clone(), None);
        let second = graph.write(&route, b"two".to_vec(), producer, None);

        assert_eq!(first[0].seq_source, 1);
        assert_eq!(second[0].seq_source, 2);
        assert_eq!(
            graph.query(QueryKindCore::Latest(route.clone())),
            QueryResultCore::Latest(Some(second[0].clone()))
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
        let left = sample_route(
            Plane::Read,
            Layer::Raw,
            "imu",
            "imu",
            "accel",
            Variant::Meta,
        );
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

        let QueryResultCore::ValidateGraph(issues) = graph.query(QueryKindCore::ValidateGraph)
        else {
            panic!("unexpected query result");
        };
        assert_eq!(issues.len(), 1);
    }

    #[test]
    fn validate_graph_reports_schema_mismatches_by_route_display() {
        let z_source = sample_route(
            Plane::Read,
            Layer::Raw,
            "zeta",
            "imu",
            "accel",
            Variant::Meta,
        );
        let a_source = sample_route(
            Plane::Read,
            Layer::Raw,
            "alpha",
            "imu",
            "accel",
            Variant::Meta,
        );
        let mut z_sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "zeta",
            "pose",
            "estimate",
            Variant::Meta,
        );
        let mut a_sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "alpha",
            "pose",
            "estimate",
            Variant::Meta,
        );
        z_sink.schema.schema_id = "Pose".to_string();
        a_sink.schema.schema_id = "Pose".to_string();

        let mut graph = GraphCore::default();
        graph.connect(&z_source, &z_sink);
        graph.connect(&a_source, &a_sink);

        let QueryResultCore::ValidateGraph(issues) = graph.query(QueryKindCore::ValidateGraph)
        else {
            panic!("unexpected query result");
        };
        assert_eq!(
            issues,
            vec![
                format!(
                    "Schema mismatch: {} -> {}",
                    a_source.display(),
                    a_sink.display()
                ),
                format!(
                    "Schema mismatch: {} -> {}",
                    z_source.display(),
                    z_sink.display()
                ),
            ]
        );
    }

    #[test]
    fn topology_reports_edges_by_route_display() {
        let z_source = sample_route(
            Plane::Read,
            Layer::Logical,
            "zeta",
            "topology",
            "source",
            Variant::Meta,
        );
        let z_sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "zeta",
            "topology",
            "sink",
            Variant::Meta,
        );
        let a_source = sample_route(
            Plane::Read,
            Layer::Logical,
            "alpha",
            "topology",
            "source",
            Variant::Meta,
        );
        let a_sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "alpha",
            "topology",
            "sink",
            Variant::Meta,
        );

        let mut graph = GraphCore::default();
        graph.connect(&z_source, &z_sink);
        graph.connect(&a_source, &a_sink);

        let QueryResultCore::Topology(edges) = graph.query(QueryKindCore::Topology) else {
            panic!("unexpected query result");
        };
        assert_eq!(
            edges,
            vec![
                (a_source.display(), a_sink.display()),
                (z_source.display(), z_sink.display()),
            ]
        );
    }

    #[test]
    fn connect_is_idempotent_for_core_fanout() {
        let source = sample_route(
            Plane::Read,
            Layer::Logical,
            "sensor",
            "telemetry",
            "source",
            Variant::Meta,
        );
        let sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "sensor",
            "telemetry",
            "sink",
            Variant::Meta,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "app".to_string(),
            kind: ProducerKind::Application,
        };

        assert!(graph.connect(&source, &sink));
        assert!(!graph.connect(&source, &sink));
        let emitted = graph.write(&source, b"sample".to_vec(), producer, None);

        assert_eq!(emitted.len(), 2);
        let QueryResultCore::Topology(edges) = graph.query(QueryKindCore::Topology) else {
            panic!("unexpected query result");
        };
        assert_eq!(edges, vec![(source.display(), sink.display())]);
        let QueryResultCore::Latest(Some(latest)) = graph.query(QueryKindCore::Latest(sink)) else {
            panic!("unexpected latest response");
        };
        assert_eq!(latest.seq_source, 1);
    }

    #[test]
    fn trace_orders_same_sequence_events_by_route_display() {
        let alpha = sample_route(
            Plane::Read,
            Layer::Logical,
            "alpha",
            "telemetry",
            "sample",
            Variant::Meta,
        );
        let beta = sample_route(
            Plane::Read,
            Layer::Logical,
            "beta",
            "telemetry",
            "sample",
            Variant::Meta,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "app".to_string(),
            kind: ProducerKind::Application,
        };

        graph.write(&beta, b"beta".to_vec(), producer.clone(), None);
        graph.write(&alpha, b"alpha".to_vec(), producer, None);

        let QueryResultCore::Trace(items) = graph.query(QueryKindCore::Trace) else {
            panic!("unexpected query result");
        };
        let displays = items
            .iter()
            .map(|item| item.route.display())
            .collect::<Vec<_>>();
        assert_eq!(displays, vec![alpha.display(), beta.display()]);
    }

    #[test]
    fn topology_query_orders_edges_by_display() {
        let z_source = sample_route(
            Plane::Read,
            Layer::Logical,
            "zeta",
            "telemetry",
            "sample",
            Variant::Meta,
        );
        let z_sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "zeta",
            "telemetry",
            "projected",
            Variant::Meta,
        );
        let a_source = sample_route(
            Plane::Read,
            Layer::Logical,
            "alpha",
            "telemetry",
            "sample",
            Variant::Meta,
        );
        let a_sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "alpha",
            "telemetry",
            "projected",
            Variant::Meta,
        );
        let mut graph = GraphCore::default();

        graph.connect(&z_source, &z_sink);
        graph.connect(&a_source, &a_sink);

        let QueryResultCore::Topology(edges) = graph.query(QueryKindCore::Topology) else {
            panic!("unexpected query result");
        };
        assert_eq!(
            edges,
            vec![
                (a_source.display(), a_sink.display()),
                (z_source.display(), z_sink.display()),
            ]
        );
    }

    #[test]
    fn validate_graph_reports_mailboxes_by_name() {
        let mut graph = GraphCore::default();
        for name in ["zeta", "alpha"] {
            let ingress = sample_route(
                Plane::Write,
                Layer::Internal,
                "mailbox",
                "mailbox",
                name,
                Variant::Request,
            );
            let egress = sample_route(
                Plane::Read,
                Layer::Internal,
                "mailbox",
                "mailbox",
                name,
                Variant::Meta,
            );
            graph.register_mailbox(
                name.to_string(),
                MailboxCore {
                    ingress,
                    egress,
                    descriptor: MailboxDescriptorCore {
                        delivery_mode: DeliveryMode::MpscSerial,
                        ordering_policy: OrderingPolicy::Fifo,
                        overflow_policy: OverflowPolicy::Block,
                        capacity: 0,
                    },
                    queue: VecDeque::new(),
                    blocked_writes: 0,
                    dropped_messages: 0,
                    coalesced_messages: 0,
                    delivered_messages: 0,
                    largest_queue_depth: 0,
                },
            );
        }

        let QueryResultCore::ValidateGraph(issues) = graph.query(QueryKindCore::ValidateGraph)
        else {
            panic!("unexpected query result");
        };
        assert_eq!(
            issues,
            vec![
                "Mailbox alpha must declare positive capacity",
                "Mailbox zeta must declare positive capacity",
            ]
        );
    }

    #[test]
    fn write_binding_updates_desired_shadow() {
        let request = sample_route(
            Plane::Write,
            Layer::Logical,
            "counter",
            "counter",
            "value",
            Variant::Request,
        );
        let desired = sample_route(
            Plane::Write,
            Layer::Shadow,
            "counter",
            "counter",
            "value",
            Variant::Desired,
        );
        let reported = sample_route(
            Plane::Write,
            Layer::Shadow,
            "counter",
            "counter",
            "value",
            Variant::Reported,
        );
        let effective = sample_route(
            Plane::Write,
            Layer::Shadow,
            "counter",
            "counter",
            "value",
            Variant::Effective,
        );
        let mut graph = GraphCore::default();
        graph.register_binding(
            "counter".to_string(),
            WriteBindingCore {
                request: request.clone(),
                desired: desired.clone(),
                reported,
                effective,
                ack: None,
            },
        );

        let producer = ProducerRefCore {
            producer_id: "app".to_string(),
            kind: ProducerKind::Application,
        };
        let emitted = graph.write(&request, b"42".to_vec(), producer, None);

        assert_eq!(emitted.len(), 2);
        assert_eq!(
            graph
                .open_latest(&desired)
                .expect("desired payload should open")
                .payload,
            b"42".to_vec()
        );
    }

    #[test]
    fn mailbox_blocks_when_full_without_consumer() {
        let ingress = sample_route(
            Plane::Write,
            Layer::Internal,
            "mailbox",
            "mailbox",
            "work",
            Variant::Request,
        );
        let egress = sample_route(
            Plane::Read,
            Layer::Internal,
            "mailbox",
            "mailbox",
            "work",
            Variant::Meta,
        );
        let mut graph = GraphCore::default();
        graph.register_mailbox(
            "work".to_string(),
            MailboxCore {
                ingress: ingress.clone(),
                egress: egress.clone(),
                descriptor: MailboxDescriptorCore {
                    delivery_mode: DeliveryMode::MpscSerial,
                    ordering_policy: OrderingPolicy::Fifo,
                    overflow_policy: OverflowPolicy::Block,
                    capacity: 1,
                },
                queue: VecDeque::new(),
                blocked_writes: 0,
                dropped_messages: 0,
                coalesced_messages: 0,
                delivered_messages: 0,
                largest_queue_depth: 0,
            },
        );
        let producer = ProducerRefCore {
            producer_id: "p1".to_string(),
            kind: ProducerKind::Application,
        };

        let first = graph.write(&ingress, b"one".to_vec(), producer.clone(), None);
        let second = graph.write(&ingress, b"two".to_vec(), producer, None);

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        let snapshot = graph.credit_snapshot();
        let ingress_snapshot = snapshot
            .into_iter()
            .find(|item| item.route_display == ingress.display())
            .unwrap();
        assert_eq!(ingress_snapshot.available, 0);
        assert_eq!(ingress_snapshot.blocked_senders, 1);
        assert_eq!(ingress_snapshot.largest_queue_depth, 1);
    }

    #[test]
    fn mailbox_route_credit_uses_name_ordered_tie_breaker() {
        let ingress = sample_route(
            Plane::Write,
            Layer::Internal,
            "mailbox",
            "mailbox",
            "shared",
            Variant::Request,
        );
        let z_egress = sample_route(
            Plane::Read,
            Layer::Internal,
            "mailbox",
            "mailbox",
            "z-egress",
            Variant::Meta,
        );
        let a_egress = sample_route(
            Plane::Read,
            Layer::Internal,
            "mailbox",
            "mailbox",
            "a-egress",
            Variant::Meta,
        );
        let mut graph = GraphCore::default();
        graph.register_mailbox(
            "zeta".to_string(),
            MailboxCore {
                ingress: ingress.clone(),
                egress: z_egress,
                descriptor: MailboxDescriptorCore {
                    delivery_mode: DeliveryMode::MpscSerial,
                    ordering_policy: OrderingPolicy::Fifo,
                    overflow_policy: OverflowPolicy::Block,
                    capacity: 7,
                },
                queue: VecDeque::new(),
                blocked_writes: 0,
                dropped_messages: 0,
                coalesced_messages: 0,
                delivered_messages: 0,
                largest_queue_depth: 0,
            },
        );
        graph.register_mailbox(
            "alpha".to_string(),
            MailboxCore {
                ingress: ingress.clone(),
                egress: a_egress,
                descriptor: MailboxDescriptorCore {
                    delivery_mode: DeliveryMode::MpscSerial,
                    ordering_policy: OrderingPolicy::Fifo,
                    overflow_policy: OverflowPolicy::Block,
                    capacity: 3,
                },
                queue: VecDeque::new(),
                blocked_writes: 0,
                dropped_messages: 0,
                coalesced_messages: 0,
                delivered_messages: 0,
                largest_queue_depth: 0,
            },
        );

        let snapshot = graph.credit_snapshot();
        let ingress_snapshot = snapshot
            .into_iter()
            .find(|item| item.route_display == ingress.display())
            .unwrap();

        assert_eq!(ingress_snapshot.available, 3);
        assert_eq!(ingress_snapshot.largest_queue_depth, 0);
    }

    #[test]
    fn write_binding_requires_shadow_coherence() {
        let request = sample_route(
            Plane::Write,
            Layer::Logical,
            "light",
            "brightness",
            "set",
            Variant::Request,
        );
        let desired = sample_route(
            Plane::Write,
            Layer::Shadow,
            "light",
            "brightness",
            "set",
            Variant::Desired,
        );
        let reported = sample_route(
            Plane::Write,
            Layer::Shadow,
            "light",
            "brightness",
            "set",
            Variant::Reported,
        );
        let effective = sample_route(
            Plane::Write,
            Layer::Shadow,
            "light",
            "brightness",
            "set",
            Variant::Effective,
        );

        let binding = WriteBindingCore {
            request: request.clone(),
            desired,
            reported,
            effective,
            ack: None,
        };
        assert!(binding.validate().is_ok());

        let invalid = WriteBindingCore {
            request,
            desired: sample_route(
                Plane::Read,
                Layer::Logical,
                "light",
                "brightness",
                "set",
                Variant::Desired,
            ),
            reported: sample_route(
                Plane::Write,
                Layer::Shadow,
                "light",
                "brightness",
                "set",
                Variant::Reported,
            ),
            effective: sample_route(
                Plane::Write,
                Layer::Shadow,
                "light",
                "brightness",
                "set",
                Variant::Effective,
            ),
            ack: None,
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn write_keeps_payload_out_of_closed_envelope_until_opened() {
        let route = sample_route(
            Plane::Read,
            Layer::Bulk,
            "lidar",
            "scan",
            "frame",
            Variant::Meta,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "lidar".to_string(),
            kind: ProducerKind::Device,
        };

        let closed = graph
            .write(&route, b"point-cloud".to_vec(), producer, None)
            .into_iter()
            .next()
            .expect("write should emit a closed envelope");
        assert!(closed.payload_ref.inline_bytes.is_empty());

        let opened = graph
            .open_latest(&route)
            .expect("payload should open from store");
        assert_eq!(opened.payload, b"point-cloud".to_vec());
        assert_eq!(graph.route_audit.get(&route).expect("audit trail").len(), 2);
    }

    #[test]
    fn descriptor_derives_route_specific_defaults() {
        let bulk = sample_route(
            Plane::Read,
            Layer::Bulk,
            "lidar",
            "scan",
            "frame",
            Variant::Meta,
        );
        let ephemeral = sample_route(
            Plane::Read,
            Layer::Ephemeral,
            "session",
            "trace",
            "entropy",
            Variant::Event,
        );
        let write = sample_route(
            Plane::Write,
            Layer::Logical,
            "motor",
            "speed",
            "set",
            Variant::Request,
        );

        let bulk_descriptor = PortDescriptorCore::for_route(&bulk);
        let ephemeral_descriptor = PortDescriptorCore::for_route(&ephemeral);
        let write_descriptor = PortDescriptorCore::for_route(&write);

        assert_eq!(bulk_descriptor.schema.payload_open_policy, "lazy_external");
        assert_eq!(
            bulk_descriptor.retention.payload_retention_policy,
            "external_store"
        );
        assert!(!bulk_descriptor.debug.payload_peek_allowed);
        assert_eq!(ephemeral_descriptor.retention.latest_replay_policy, "none");
        assert_eq!(
            ephemeral_descriptor.environment.ephemeral_scope,
            Some("session".to_string())
        );
        assert_eq!(
            write_descriptor.time.clock_domain.clock_domain_id,
            "control_epoch"
        );
        assert_eq!(write_descriptor.flow.credit_class, "control");
    }
}
