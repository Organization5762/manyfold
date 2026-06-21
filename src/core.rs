#![allow(clippy::too_many_arguments)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_ROUTE_HISTORY_LIMIT: usize = 8;

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
pub struct RetentionPolicyCore {
    pub latest_replay_policy: String,
    pub durability_class: String,
    pub replay_window: String,
    pub payload_retention_policy: String,
    pub history_limit: Option<usize>,
    pub lineage_retention_policy: String,
}

impl RetentionPolicyCore {
    fn for_route(route: &RouteRefCore) -> Self {
        match route.namespace.layer {
            Layer::Ephemeral => Self {
                latest_replay_policy: "none".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "none".to_string(),
                payload_retention_policy: "non_replayable".to_string(),
                history_limit: Some(1),
                lineage_retention_policy: "none".to_string(),
            },
            Layer::Internal => Self {
                latest_replay_policy: "latest_only".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "latest".to_string(),
                payload_retention_policy: "separate_store".to_string(),
                history_limit: Some(1),
                lineage_retention_policy: "none".to_string(),
            },
            _ => Self {
                latest_replay_policy: "bounded_history".to_string(),
                durability_class: "memory".to_string(),
                replay_window: format!("last_{DEFAULT_ROUTE_HISTORY_LIMIT}"),
                payload_retention_policy: if route.namespace.layer == Layer::Bulk {
                    "external_store".to_string()
                } else {
                    "separate_store".to_string()
                },
                history_limit: Some(DEFAULT_ROUTE_HISTORY_LIMIT),
                lineage_retention_policy: "none".to_string(),
            },
        }
    }
}

enum RetentionPolicyRef<'policy> {
    Borrowed(&'policy RetentionPolicyCore),
    Owned(RetentionPolicyCore),
}

impl<'policy> RetentionPolicyRef<'policy> {
    fn as_ref(&'policy self) -> &'policy RetentionPolicyCore {
        match self {
            Self::Borrowed(policy) => policy,
            Self::Owned(policy) => policy,
        }
    }
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
pub struct RetentionSnapshotCore {
    pub route_display: String,
    pub latest_seq_source: Option<u64>,
    pub metadata_event_count: u64,
    pub replay_count: usize,
    pub payload_count: usize,
    pub lineage_count: usize,
    pub trace_index_count: usize,
    pub causality_index_count: usize,
    pub correlation_index_count: usize,
    pub history_limit: Option<usize>,
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
        let epoch_id = fixed_width_sequence(self.epoch);
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
            causality_id: Some(format!("{}#{}", self.name, epoch_id)),
            correlation_id: Some(format!("control-epoch-{}", epoch_id)),
            trace_id: Some(format!("trace:{}:{}", self.name, epoch_id)),
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
                payload_id: format!("control-loop-{}", epoch_id),
                logical_length_bytes: 0,
                codec_id: "identity".to_string(),
                inline_bytes: Vec::new(),
            },
            qos_class: "control".to_string(),
            security_labels: vec!["write".to_string()],
        }
    }
}

fn fixed_width_sequence(value: u64) -> String {
    format!("{value:020}")
}

fn event_payload_id(route_display: &str, seq_source: u64) -> String {
    format!("{route_display}:{seq_source:020}")
}

fn control_epoch_correlation_id(epoch: u64) -> String {
    format!("control-epoch-{epoch:020}")
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
#[allow(clippy::large_enum_variant)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageRecordCore {
    pub event_route_display: String,
    pub event_seq_source: u64,
    pub producer_id: Option<String>,
    pub trace_id: String,
    pub causality_id: String,
    pub correlation_id: Option<String>,
    pub parent_events: Vec<(String, u64)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedLineageCore {
    pub producer_id: Option<String>,
    pub trace_id: LineageIdCore,
    pub causality_id: LineageIdCore,
    pub correlation_id: Option<String>,
    pub parent_events: Vec<RetainedParentEventCore>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetainedParentEventCore {
    RouteId(usize, u64),
    Display(String, u64),
}

type EventKeyCore = (usize, u64);
type LineageIdCore = usize;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CorrelationStoreModeCore {
    Retained,
    #[default]
    Noop,
}

#[derive(Default)]
pub struct LineageStoreCore {
    /// Compact route ids let retained parent lineage avoid per-event display clones.
    pub route_ids: HashMap<RouteRefCore, usize>,
    /// Reverse lookup for public lineage records that expose display strings.
    pub routes_by_id: Vec<RouteRefCore>,
    /// Lineage id lookup interns repeated retained trace/causality strings.
    pub lineage_ids_by_value: HashMap<String, LineageIdCore>,
    /// Reverse lookup for public lineage records that expose retained lineage strings.
    pub lineage_values_by_id: Vec<Option<String>>,
    /// Retained lineage reference counts release dynamic interned ids after expiry.
    pub lineage_id_ref_counts: Vec<usize>,
    /// Vacated lineage ids are reused so unique trace/causality values stay bounded by retention.
    pub free_lineage_ids: Vec<LineageIdCore>,
    /// Lineage records live only while their event is latest or inside retained replay.
    pub lineage_by_event: HashMap<EventKeyCore, RetainedLineageCore>,
    /// Trace indexes mirror retained lineage records and are cleared with the record.
    pub lineage_events_by_trace: HashMap<LineageIdCore, Vec<EventKeyCore>>,
    /// Causality indexes mirror retained lineage records and are cleared with the record.
    pub lineage_events_by_causality: HashMap<LineageIdCore, Vec<EventKeyCore>>,
    /// Correlation indexes mirror retained lineage records and are cleared with the record.
    pub lineage_events_by_correlation: HashMap<String, Vec<EventKeyCore>>,
    /// Lineage-retained routes mirror retention policies for hot lineage writes.
    pub lineage_retained_routes: HashSet<RouteRefCore>,
}

#[derive(Default)]
pub struct GraphCore {
    /// Route descriptors live for the registered graph topology.
    pub descriptors: HashMap<RouteRefCore, PortDescriptorCore>,
    /// Display-name lookup lives with topology and avoids per-event descriptor scans.
    pub routes_by_display: HashMap<String, RouteRefCore>,
    /// Route display names live with topology so hot writes avoid repeated formatting.
    pub route_displays: HashMap<RouteRefCore, String>,
    /// Latest envelopes live until the route receives a newer envelope or the graph is dropped.
    pub latest: HashMap<RouteRefCore, ClosedEnvelopeCore>,
    /// Replay history lives under per-route retention and is trimmed after each write.
    pub history: HashMap<RouteRefCore, VecDeque<ClosedEnvelopeCore>>,
    /// Payload bytes live only while their envelope remains retained for the owning route.
    pub payloads: HashMap<String, Arc<[u8]>>,
    /// Route audit entries live only while their referenced write remains retained.
    pub route_audit: HashMap<RouteRefCore, Vec<AuditEventCore>>,
    /// Retained lineage store is allocated only when explicitly attached.
    pub lineage: Option<LineageStoreCore>,
    /// Correlation store mode controls optional debug/introspection correlation indexes.
    pub correlation_store_mode: CorrelationStoreModeCore,
    /// Retention policies live for the registered route and bound history/payload/audit state.
    pub retention_policies: HashMap<RouteRefCore, RetentionPolicyCore>,
    /// History limits mirror retention policies and are copied on hot writes.
    pub retention_history_limits: HashMap<RouteRefCore, Option<usize>>,
    /// Edges live for explicit topology connections until disconnect.
    pub edges: Vec<(RouteRefCore, RouteRefCore)>,
    /// Outgoing edge indexes mirror `edges` so hot writes avoid topology scans.
    pub outgoing_edges_by_source: HashMap<RouteRefCore, Vec<RouteRefCore>>,
    /// Routed route membership mirrors topology so sparse writes use one hot lookup.
    pub routed_routes: HashSet<RouteRefCore>,
    /// Native materialize edges live only while their owning Python subscription lives.
    pub materialize_targets_by_source: HashMap<RouteRefCore, Vec<RouteRefCore>>,
    /// Materializer generation invalidates compiled route-pair profiles on topology change.
    pub materialize_generation: u64,
    pub bindings: BTreeMap<String, WriteBindingCore>,
    pub request_bindings: HashMap<RouteRefCore, WriteBindingCore>,
    pub mailboxes: BTreeMap<String, MailboxCore>,
    /// Mailbox ingress lookup lives with mailbox topology for O(1) route checks.
    pub mailbox_egress_by_ingress: HashMap<RouteRefCore, RouteRefCore>,
    /// Mailbox egress lookup lets demand-shaped drain find its queue without scans.
    pub mailbox_name_by_egress: HashMap<RouteRefCore, String>,
    pub loops: BTreeMap<String, ControlLoopCore>,
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
        let audit = self.route_audit.entry(route.clone()).or_default();
        if let Some(event) = audit
            .iter_mut()
            .find(|event| event.action == action && event.actor_id == actor_id)
        {
            event.payload_id = payload_id;
            event.seq_source = seq_source;
            if audit.len() == 1 {
                return;
            }
        } else {
            audit.push(AuditEventCore {
                route: route.clone(),
                action: action.to_string(),
                payload_id,
                actor_id: actor_id.to_string(),
                seq_source,
            });
        }
        if audit.len() > 1 {
            self.enforce_route_audit_retention(route);
        }
    }

    fn append_write_audit(&mut self, route: &RouteRefCore, actor_id: &str, seq_source: u64) {
        let audit = self.route_audit.entry(route.clone()).or_default();
        let mut updated = false;
        if let Some(event) = audit.first_mut() {
            if event.action == "write" && event.actor_id == actor_id {
                event.payload_id = None;
                event.seq_source = Some(seq_source);
                updated = true;
            }
        }
        if !updated {
            if let Some(event) = audit
                .iter_mut()
                .find(|event| event.action == "write" && event.actor_id == actor_id)
            {
                event.payload_id = None;
                event.seq_source = Some(seq_source);
                updated = true;
            }
        }
        if !updated {
            audit.push(AuditEventCore {
                route: route.clone(),
                action: "write".to_string(),
                payload_id: None,
                actor_id: actor_id.to_string(),
                seq_source: Some(seq_source),
            });
        }
        if audit.len() == 1 {
            return;
        }
        if self
            .route_audit
            .get(route)
            .is_some_and(|audit| audit.len() > 1)
        {
            self.enforce_route_audit_retention(route);
        }
    }

    fn native_history_limit(policy: &RetentionPolicyCore) -> Option<usize> {
        match policy.latest_replay_policy.as_str() {
            "none" => Some(0),
            "latest_only" => Some(1),
            "bounded_history" => policy.history_limit,
            _ => None,
        }
    }

    fn retained_event_keys_for_route(&self, route: &RouteRefCore) -> Vec<EventKeyCore> {
        let Some(route_id) = self.optional_route_id(route) else {
            return Vec::new();
        };
        let mut retained = Vec::new();
        if let Some(latest) = self.latest.get(route) {
            retained.push((route_id, latest.seq_source));
        }
        if let Some(history) = self.history.get(route) {
            for envelope in history {
                let event_key = (route_id, envelope.seq_source);
                if !retained.contains(&event_key) {
                    retained.push(event_key);
                }
            }
        }
        retained
    }

    fn route_retains_event(&self, route: &RouteRefCore, seq_source: u64) -> bool {
        if self
            .latest
            .get(route)
            .is_some_and(|latest| latest.seq_source == seq_source)
        {
            return true;
        }
        self.history.get(route).is_some_and(|history| {
            history
                .iter()
                .any(|envelope| envelope.seq_source == seq_source)
        })
    }

    fn retained_envelope_for_route_event(
        &self,
        route: &RouteRefCore,
        seq_source: u64,
    ) -> Option<ClosedEnvelopeCore> {
        if let Some(latest) = self.latest.get(route) {
            if latest.seq_source == seq_source {
                return Some(latest.clone());
            }
        }
        self.history.get(route).and_then(|history| {
            history
                .iter()
                .find(|envelope| envelope.seq_source == seq_source)
                .cloned()
        })
    }

    fn route_display(&self, route: &RouteRefCore) -> String {
        self.route_displays
            .get(route)
            .cloned()
            .unwrap_or_else(|| route.display())
    }

    fn registered_route_display<'a>(&'a self, route: &'a RouteRefCore) -> &'a str {
        self.route_displays
            .get(route)
            .map(String::as_str)
            .expect("registered route display missing")
    }

    fn lineage_store(&self) -> Option<&LineageStoreCore> {
        self.lineage.as_ref()
    }

    fn lineage_store_mut(&mut self) -> Option<&mut LineageStoreCore> {
        self.lineage.as_mut()
    }

    fn ensure_lineage_store(&mut self) -> &mut LineageStoreCore {
        if self.lineage.is_none() {
            self.lineage = Some(LineageStoreCore::default());
        }
        self.lineage
            .as_mut()
            .expect("retained lineage store missing")
    }

    fn ensure_lineage_route_id(&mut self, route: &RouteRefCore) -> usize {
        if let Some(route_id) = self
            .lineage_store()
            .and_then(|lineage| lineage.route_ids.get(route).copied())
        {
            return route_id;
        }
        let route = route.clone();
        let lineage = self.ensure_lineage_store();
        let route_id = lineage.routes_by_id.len();
        lineage.route_ids.insert(route.clone(), route_id);
        lineage.routes_by_id.push(route);
        route_id
    }

    fn registered_route_id(&self, route: &RouteRefCore) -> usize {
        self.lineage_store()
            .and_then(|lineage| lineage.route_ids.get(route).copied())
            .expect("registered route id missing")
    }

    fn optional_route_id(&self, route: &RouteRefCore) -> Option<usize> {
        self.lineage_store()
            .and_then(|lineage| lineage.route_ids.get(route).copied())
    }

    fn retained_route_id(&mut self, route: &RouteRefCore) -> usize {
        self.ensure_lineage_route_id(route)
    }

    fn retained_event_key(&mut self, route: &RouteRefCore, seq_source: u64) -> EventKeyCore {
        (self.retained_route_id(route), seq_source)
    }

    fn event_key_route(&self, event_key: &EventKeyCore) -> &RouteRefCore {
        self.lineage_store()
            .and_then(|lineage| lineage.routes_by_id.get(event_key.0))
            .expect("retained event route id missing")
    }

    fn event_key_display(&self, event_key: &EventKeyCore) -> String {
        self.route_display(self.event_key_route(event_key))
    }

    fn intern_lineage_id(&mut self, value: &str) -> LineageIdCore {
        if let Some(lineage_id) = self
            .lineage_store()
            .and_then(|lineage| lineage.lineage_ids_by_value.get(value).copied())
        {
            return lineage_id;
        }
        let lineage = self.ensure_lineage_store();
        let lineage_id = lineage
            .free_lineage_ids
            .pop()
            .unwrap_or(lineage.lineage_values_by_id.len());
        if lineage_id == lineage.lineage_values_by_id.len() {
            lineage.lineage_values_by_id.push(Some(value.to_string()));
            lineage.lineage_id_ref_counts.push(0);
        } else {
            lineage.lineage_values_by_id[lineage_id] = Some(value.to_string());
        }
        lineage
            .lineage_ids_by_value
            .insert(value.to_string(), lineage_id);
        lineage_id
    }

    fn lineage_value(&self, lineage_id: LineageIdCore) -> String {
        self.lineage_store()
            .and_then(|lineage| lineage.lineage_values_by_id.get(lineage_id))
            .and_then(Option::as_ref)
            .expect("retained lineage id missing")
            .clone()
    }

    pub fn lineage_id_for_value(&self, value: &str) -> Option<LineageIdCore> {
        self.lineage_store()
            .and_then(|lineage| lineage.lineage_ids_by_value.get(value).copied())
    }

    pub fn active_lineage_value_count(&self) -> usize {
        self.lineage_store().map_or(0, |lineage| {
            lineage
                .lineage_values_by_id
                .iter()
                .filter(|lineage_value| lineage_value.is_some())
                .count()
        })
    }

    pub fn retained_lineage_event_count(&self) -> usize {
        self.lineage_store()
            .map_or(0, |lineage| lineage.lineage_by_event.len())
    }

    pub fn retain_lineage_profile(
        &mut self,
        trace_id: &str,
        causality_id: &str,
    ) -> (LineageIdCore, LineageIdCore) {
        let trace_id = self.intern_lineage_id(trace_id);
        let causality_id = self.intern_lineage_id(causality_id);
        self.retain_lineage_id(trace_id);
        self.retain_lineage_id(causality_id);
        (trace_id, causality_id)
    }

    pub fn release_lineage_profile_ids(
        &mut self,
        trace_id: LineageIdCore,
        causality_id: LineageIdCore,
    ) {
        self.release_lineage_id(trace_id);
        self.release_lineage_id(causality_id);
    }

    fn lineage_id_is_active(&self, lineage_id: LineageIdCore) -> bool {
        self.lineage_store().is_some_and(|lineage| {
            lineage
                .lineage_values_by_id
                .get(lineage_id)
                .is_some_and(Option::is_some)
        })
    }

    fn retain_lineage_id(&mut self, lineage_id: LineageIdCore) {
        let ref_count = self
            .ensure_lineage_store()
            .lineage_id_ref_counts
            .get_mut(lineage_id)
            .expect("retained lineage refcount missing");
        *ref_count += 1;
    }

    fn release_lineage_id(&mut self, lineage_id: LineageIdCore) {
        let Some(lineage) = self.lineage_store_mut() else {
            return;
        };
        let Some(ref_count) = lineage.lineage_id_ref_counts.get_mut(lineage_id) else {
            return;
        };
        if *ref_count == 0 {
            return;
        }
        *ref_count -= 1;
        if *ref_count != 0 {
            return;
        }
        if let Some(value) = lineage
            .lineage_values_by_id
            .get_mut(lineage_id)
            .and_then(Option::take)
        {
            lineage.lineage_ids_by_value.remove(value.as_str());
            lineage.free_lineage_ids.push(lineage_id);
        }
    }

    fn registered_parent_event(
        &self,
        route: &RouteRefCore,
        seq_source: u64,
    ) -> RetainedParentEventCore {
        RetainedParentEventCore::RouteId(self.registered_route_id(route), seq_source)
    }

    fn retained_parent_event_list(
        &self,
        route: &RouteRefCore,
        seq_source: u64,
    ) -> Vec<RetainedParentEventCore> {
        if self.lineage_store_retains() {
            vec![self.registered_parent_event(route, seq_source)]
        } else {
            Vec::new()
        }
    }

    fn retained_parent_events_from_display(
        &self,
        parent_events: Vec<(String, u64)>,
    ) -> Vec<RetainedParentEventCore> {
        parent_events
            .into_iter()
            .map(|(route_display, seq_source)| {
                self.routes_by_display
                    .get(&route_display)
                    .and_then(|route| self.optional_route_id(route))
                    .map(|route_id| RetainedParentEventCore::RouteId(route_id, seq_source))
                    .unwrap_or(RetainedParentEventCore::Display(route_display, seq_source))
            })
            .collect()
    }

    fn retained_parent_events_to_display(
        &self,
        parent_events: &[RetainedParentEventCore],
    ) -> Vec<(String, u64)> {
        parent_events
            .iter()
            .map(|parent| match parent {
                RetainedParentEventCore::RouteId(route_id, seq_source) => {
                    let route = self
                        .lineage_store()
                        .and_then(|lineage| lineage.routes_by_id.get(*route_id))
                        .expect("retained parent route id missing");
                    (self.route_display(route), *seq_source)
                }
                RetainedParentEventCore::Display(route_display, seq_source) => {
                    (route_display.clone(), *seq_source)
                }
            })
            .collect()
    }

    fn remove_lineage_index<K>(
        index: &mut HashMap<K, Vec<EventKeyCore>>,
        lineage_id: &K,
        event_key: &EventKeyCore,
    ) where
        K: Eq + Hash,
    {
        let Some(events) = index.get_mut(lineage_id) else {
            return;
        };
        events.retain(|item| item != event_key);
        if events.is_empty() {
            index.remove(lineage_id);
        }
    }

    fn forget_lineage_event(&mut self, event_key: &EventKeyCore) {
        let Some(lineage) = self.lineage_store_mut() else {
            return;
        };
        let Some(record) = lineage.lineage_by_event.remove(event_key) else {
            return;
        };
        Self::remove_lineage_index(
            &mut lineage.lineage_events_by_trace,
            &record.trace_id,
            event_key,
        );
        Self::remove_lineage_index(
            &mut lineage.lineage_events_by_causality,
            &record.causality_id,
            event_key,
        );
        if let Some(correlation_id) = &record.correlation_id {
            Self::remove_lineage_index(
                &mut lineage.lineage_events_by_correlation,
                correlation_id,
                event_key,
            );
        }
        self.release_lineage_id(record.trace_id);
        self.release_lineage_id(record.causality_id);
    }

    fn envelope_is_retained(&self, route: &RouteRefCore, envelope: &ClosedEnvelopeCore) -> bool {
        if self
            .latest
            .get(route)
            .is_some_and(|latest| latest.payload_ref.payload_id == envelope.payload_ref.payload_id)
        {
            return true;
        }
        self.history.get(route).is_some_and(|history| {
            history
                .iter()
                .any(|retained| retained.payload_ref.payload_id == envelope.payload_ref.payload_id)
        })
    }

    fn forget_unretained_envelope(&mut self, route: &RouteRefCore, envelope: ClosedEnvelopeCore) {
        if self.envelope_is_retained(route, &envelope) {
            return;
        }
        self.forget_payload(&envelope.payload_ref.payload_id);
        if let Some(route_id) = self.optional_route_id(route) {
            self.forget_lineage_event(&(route_id, envelope.seq_source));
        }
    }

    fn enforce_route_audit_retention(&mut self, route: &RouteRefCore) {
        let oldest_history_seq = self
            .history
            .get(route)
            .and_then(|history| history.front())
            .map(|envelope| envelope.seq_source);
        let latest_seq_source = self.latest.get(route).map(|envelope| envelope.seq_source);
        let Some(audit) = self.route_audit.get_mut(route) else {
            return;
        };
        audit.retain(|event| match event.seq_source {
            Some(seq_source) => {
                Some(seq_source) == latest_seq_source
                    || oldest_history_seq.is_some_and(|oldest| seq_source >= oldest)
            }
            None => true,
        });
        if audit.is_empty() {
            self.route_audit.remove(route);
        }
    }

    fn enforce_route_retention(&mut self, route: &RouteRefCore, policy: &RetentionPolicyCore) {
        self.enforce_route_retention_limit(route, Self::native_history_limit(policy));
    }

    fn enforce_route_retention_limit(&mut self, route: &RouteRefCore, limit: Option<usize>) {
        if let Some(limit) = limit {
            loop {
                let expired = {
                    let Some(history) = self.history.get_mut(route) else {
                        break;
                    };
                    if history.len() <= limit {
                        break;
                    }
                    history.pop_front()
                };
                if let Some(envelope) = expired {
                    self.forget_unretained_envelope(route, envelope);
                }
            }
        }
        if self
            .route_audit
            .get(route)
            .is_some_and(|audit| audit.len() > 1)
        {
            self.enforce_route_audit_retention(route);
        }
    }

    fn retention_policy_for<'a>(&'a self, route: &RouteRefCore) -> RetentionPolicyRef<'a> {
        self.retention_policies
            .get(route)
            .map(RetentionPolicyRef::Borrowed)
            .unwrap_or_else(|| RetentionPolicyRef::Owned(RetentionPolicyCore::for_route(route)))
    }

    fn retention_history_limit_for(&self, route: &RouteRefCore) -> Option<usize> {
        self.retention_history_limits
            .get(route)
            .copied()
            .unwrap_or_else(|| Self::native_history_limit(&RetentionPolicyCore::for_route(route)))
    }

    fn route_has_immediate_routing_uncached(&self, route: &RouteRefCore) -> bool {
        self.request_bindings.contains_key(route)
            || self
                .outgoing_edges_by_source
                .get(route)
                .is_some_and(|sinks| !sinks.is_empty())
            || self.mailbox_egress_by_ingress.contains_key(route)
    }

    fn refresh_routed_route(&mut self, route: &RouteRefCore) {
        if self.route_has_immediate_routing_uncached(route) {
            self.routed_routes.insert(route.clone());
        } else {
            self.routed_routes.remove(route);
        }
    }

    pub fn attach_retained_lineage_store(&mut self) {
        self.ensure_lineage_store();
        let routes = self.descriptors.keys().cloned().collect::<Vec<_>>();
        for route in routes {
            self.ensure_lineage_route_id(&route);
        }
        self.rebuild_lineage_retained_routes();
    }

    pub fn attach_noop_lineage_store(&mut self) {
        self.lineage = None;
    }

    pub fn attach_retained_correlation_store(&mut self) {
        self.correlation_store_mode = CorrelationStoreModeCore::Retained;
    }

    pub fn attach_noop_correlation_store(&mut self) {
        self.correlation_store_mode = CorrelationStoreModeCore::Noop;
        if let Some(lineage) = self.lineage_store_mut() {
            lineage.lineage_events_by_correlation.clear();
            for record in lineage.lineage_by_event.values_mut() {
                record.correlation_id = None;
            }
        }
    }

    fn lineage_store_retains(&self) -> bool {
        self.lineage_store().is_some()
    }

    fn correlation_store_retains(&self) -> bool {
        matches!(
            self.correlation_store_mode,
            CorrelationStoreModeCore::Retained
        )
    }

    fn retained_correlation_id(&self, correlation_id: Option<String>) -> Option<String> {
        if self.correlation_store_retains() {
            correlation_id
        } else {
            None
        }
    }

    fn rebuild_lineage_retained_routes(&mut self) {
        let retained_routes = self
            .retention_policies
            .iter()
            .filter(|(_, policy)| policy.lineage_retention_policy == "retained")
            .map(|(route, _)| route.clone())
            .collect::<Vec<_>>();
        let Some(lineage) = self.lineage_store_mut() else {
            return;
        };
        lineage.lineage_retained_routes.clear();
        lineage.lineage_retained_routes.extend(retained_routes);
    }

    pub fn configure_retention(
        &mut self,
        route: &RouteRefCore,
        mut policy: RetentionPolicyCore,
    ) -> RetentionPolicyCore {
        self.register_port(route.clone());
        if policy.latest_replay_policy == "bounded_history" && policy.history_limit.is_none() {
            policy.history_limit = Some(DEFAULT_ROUTE_HISTORY_LIMIT);
        }
        if let Some(descriptor) = self.descriptors.get_mut(route) {
            descriptor.retention.latest_replay_policy = policy.latest_replay_policy.clone();
            descriptor.retention.durability_class = policy.durability_class.clone();
            descriptor.retention.replay_window = policy.replay_window.clone();
            descriptor.retention.payload_retention_policy = policy.payload_retention_policy.clone();
        }
        self.retention_history_limits
            .insert(route.clone(), Self::native_history_limit(&policy));
        if let Some(lineage) = self.lineage_store_mut() {
            if policy.lineage_retention_policy == "retained" {
                lineage.lineage_retained_routes.insert(route.clone());
                self.ensure_lineage_route_id(route);
            } else {
                lineage.lineage_retained_routes.remove(route);
            }
        }
        self.retention_policies
            .insert(route.clone(), policy.clone());
        self.enforce_route_retention(route, &policy);
        policy
    }

    fn event_taints(
        route: &RouteRefCore,
        route_display: &str,
        control_epoch: Option<u64>,
    ) -> Vec<TaintMarkCore> {
        let mut taints = Vec::new();
        if route.namespace.plane == Plane::Write && route.variant == Variant::Request {
            taints.push(TaintMarkCore {
                domain: TaintDomain::Coherence,
                value_id: "COHERENCE_WRITE_PENDING".to_string(),
                origin_id: route_display.to_string(),
            });
        }
        if route.namespace.layer == Layer::Ephemeral {
            taints.push(TaintMarkCore {
                domain: TaintDomain::Determinism,
                value_id: "DET_NONREPLAYABLE".to_string(),
                origin_id: route_display.to_string(),
            });
        }
        if control_epoch.is_some() {
            taints.push(TaintMarkCore {
                domain: TaintDomain::Scheduling,
                value_id: "SCHED_READY".to_string(),
                origin_id: route_display.to_string(),
            });
        }
        taints
    }

    fn route_has_static_taints(route: &RouteRefCore) -> bool {
        (route.namespace.plane == Plane::Write && route.variant == Variant::Request)
            || route.namespace.layer == Layer::Ephemeral
    }

    fn payload_bytes_for(&self, envelope: &ClosedEnvelopeCore) -> Vec<u8> {
        self.payloads
            .get(&envelope.payload_ref.payload_id)
            .map(|payload| payload.as_ref().to_vec())
            .unwrap_or_else(|| envelope.payload_ref.inline_bytes.clone())
    }

    fn remember_payload(&mut self, payload_id: &str, payload: Arc<[u8]>) {
        self.payloads.insert(payload_id.to_string(), payload);
    }

    fn forget_payload(&mut self, payload_id: &str) {
        self.payloads.remove(payload_id);
    }

    pub fn register_port(&mut self, route: RouteRefCore) -> RouteRefCore {
        if let Err(message) = Self::validate_route(&route) {
            panic!("invalid route registered: {message}");
        }
        let route_display = route.display();
        self.descriptors
            .entry(route.clone())
            .or_insert_with(|| PortDescriptorCore::for_route(&route));
        self.routes_by_display
            .entry(route_display.clone())
            .or_insert_with(|| route.clone());
        self.route_displays
            .entry(route.clone())
            .or_insert(route_display);
        if self.lineage_store_retains() {
            self.ensure_lineage_route_id(&route);
        }
        if !self.retention_policies.contains_key(&route) {
            let policy = RetentionPolicyCore::for_route(&route);
            self.retention_history_limits
                .insert(route.clone(), Self::native_history_limit(&policy));
            if let Some(lineage) = self.lineage_store_mut() {
                if policy.lineage_retention_policy == "retained" {
                    lineage.lineage_retained_routes.insert(route.clone());
                }
            }
            self.retention_policies.insert(route.clone(), policy);
        }
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
        self.routed_routes.insert(binding.request.clone());
        self.bindings.insert(name, binding);
    }

    pub fn register_mailbox(&mut self, name: String, mailbox: MailboxCore) {
        self.register_port(mailbox.ingress.clone());
        self.register_port(mailbox.egress.clone());
        self.mailbox_egress_by_ingress
            .insert(mailbox.ingress.clone(), mailbox.egress.clone());
        self.mailbox_name_by_egress
            .insert(mailbox.egress.clone(), name.clone());
        self.routed_routes.insert(mailbox.ingress.clone());
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
        self.outgoing_edges_by_source
            .entry(source.clone())
            .or_default()
            .push(sink.clone());
        self.routed_routes.insert(source.clone());
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
        if let Some(sinks) = self.outgoing_edges_by_source.get_mut(source) {
            sinks.retain(|candidate| candidate != sink);
            if sinks.is_empty() {
                self.outgoing_edges_by_source.remove(source);
            }
        }
        self.refresh_routed_route(source);
        true
    }

    pub fn register_materialize_bytes(
        &mut self,
        source: &RouteRefCore,
        target: &RouteRefCore,
    ) -> bool {
        let source = self.register_port(source.clone());
        let target = self.register_port(target.clone());
        let targets = self
            .materialize_targets_by_source
            .entry(source)
            .or_default();
        if targets.contains(&target) {
            return false;
        }
        targets.push(target);
        self.materialize_generation = self.materialize_generation.wrapping_add(1);
        true
    }

    pub fn unregister_materialize_bytes(
        &mut self,
        source: &RouteRefCore,
        target: &RouteRefCore,
    ) -> bool {
        let Some(targets) = self.materialize_targets_by_source.get_mut(source) else {
            return false;
        };
        let Some(index) = targets.iter().position(|candidate| candidate == target) else {
            return false;
        };
        targets.remove(index);
        if targets.is_empty() {
            self.materialize_targets_by_source.remove(source);
        }
        self.materialize_generation = self.materialize_generation.wrapping_add(1);
        true
    }

    pub fn materializer_pair_is_registered(
        &self,
        route: &RouteRefCore,
        target_route: &RouteRefCore,
    ) -> bool {
        self.materialize_targets_by_source
            .get(route)
            .is_some_and(|targets| targets.iter().any(|target| target == target_route))
    }

    fn prepare_direct_envelope_with_shared_payload(
        &mut self,
        route: &RouteRefCore,
        payload: Arc<[u8]>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> ClosedEnvelopeCore {
        if !self.descriptors.contains_key(route) {
            self.register_port(route.clone());
        }
        let (clock_domain, credit_class, read_capabilities) = {
            let descriptor = self
                .descriptors
                .get(route)
                .expect("registered route descriptor missing");
            (
                descriptor.time.clock_domain.clone(),
                descriptor.flow.credit_class.clone(),
                descriptor.security.read_capabilities.clone(),
            )
        };
        let seq_source = self
            .latest
            .get(route)
            .map(|envelope| {
                envelope
                    .seq_source
                    .checked_add(1)
                    .expect("route sequence overflowed u64")
            })
            .unwrap_or(1);
        let (payload_id, correlation_id, taints) = {
            let route_display = self.registered_route_display(route);
            (
                event_payload_id(route_display, seq_source),
                control_epoch.map(control_epoch_correlation_id),
                Self::event_taints(route, route_display, control_epoch),
            )
        };
        let payload_len = payload.len() as u64;
        let now = Self::now_unix_ms();
        self.remember_payload(&payload_id, payload);
        ClosedEnvelopeCore {
            route: route.clone(),
            producer,
            emitter: RuntimeRefCore {
                runtime_id: "runtime:in_memory".to_string(),
            },
            seq_source,
            link_seq: None,
            device_time_unix_ms: None,
            ingest_time_unix_ms: now,
            logical_time_unix_ms: now,
            logical_clock_domain: clock_domain,
            partition_key: None,
            causality_id: None,
            correlation_id,
            trace_id: None,
            control_epoch,
            taints,
            guards: Vec::new(),
            payload_ref: PayloadRefCore {
                payload_id,
                logical_length_bytes: payload_len,
                codec_id: "identity".to_string(),
                inline_bytes: Vec::new(),
            },
            qos_class: credit_class,
            security_labels: read_capabilities,
        }
    }

    fn prepare_direct_envelope(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> ClosedEnvelopeCore {
        self.prepare_direct_envelope_with_shared_payload(
            route,
            Arc::from(payload),
            producer,
            control_epoch,
        )
    }

    fn direct_write(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> ClosedEnvelopeCore {
        let envelope = self.prepare_direct_envelope(route, payload, producer, control_epoch);
        let seq_source = envelope.seq_source;
        let replaced_latest = self.latest.insert(route.clone(), envelope.clone());
        self.history
            .entry(route.clone())
            .or_default()
            .push_back(envelope.clone());
        self.append_write_audit(route, &envelope.producer.producer_id, seq_source);
        self.enforce_route_retention_limit(route, self.retention_history_limit_for(route));
        if let Some(replaced) = replaced_latest {
            self.forget_unretained_envelope(route, replaced);
        }
        envelope
    }

    fn direct_write_drop(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> u64 {
        self.direct_write_drop_shared_payload(route, Arc::from(payload), producer, control_epoch)
    }

    fn direct_write_drop_shared_payload(
        &mut self,
        route: &RouteRefCore,
        payload: Arc<[u8]>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> u64 {
        let envelope = self.prepare_direct_envelope_with_shared_payload(
            route,
            payload,
            producer,
            control_epoch,
        );
        let seq_source = envelope.seq_source;
        self.append_write_audit(route, &envelope.producer.producer_id, seq_source);
        let history_envelope = envelope.clone();
        let replaced_latest = self.latest.insert(route.clone(), envelope);
        self.history
            .entry(route.clone())
            .or_default()
            .push_back(history_envelope);
        self.enforce_route_retention_limit(route, self.retention_history_limit_for(route));
        if let Some(replaced) = replaced_latest {
            self.forget_unretained_envelope(route, replaced);
        }
        seq_source
    }

    fn route_credit_snapshot(&self, route: &RouteRefCore) -> CreditSnapshotCore {
        let descriptor = self
            .descriptors
            .get(route)
            .cloned()
            .unwrap_or_else(|| PortDescriptorCore::for_route(route));
        let route_display = self.route_display(route);
        let maybe_mailbox = self
            .mailboxes
            .values()
            .find(|mailbox| mailbox.ingress == *route || mailbox.egress == *route);

        match maybe_mailbox {
            Some(mailbox) => CreditSnapshotCore {
                route_display,
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
                route_display,
                credit_class: descriptor.flow.credit_class,
                available: u64::MAX,
                blocked_senders: 0,
                dropped_messages: 0,
                largest_queue_depth: self.history.get(route).map_or(0, VecDeque::len),
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
        let mailbox = self
            .mailboxes
            .values_mut()
            .find(|mailbox| mailbox.ingress == *route)?;
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
        let Some(name) = self.mailbox_name_by_egress.get(source).cloned() else {
            return Vec::new();
        };

        let has_consumer = self
            .outgoing_edges_by_source
            .get(source)
            .is_some_and(|sinks| !sinks.is_empty());
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
                .outgoing_edges_by_source
                .get(&route)
                .cloned()
                .unwrap_or_default();

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
                            producer_id: self.route_display(&binding.request),
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

        if let Some(egress) = self.try_enqueue_mailbox(&route, envelope) {
            emitted.extend(self.try_drain_mailbox_for_source(&egress));
        }

        emitted
    }

    fn route_has_immediate_routing(&self, route: &RouteRefCore) -> bool {
        self.routed_routes.contains(route)
    }

    pub fn write(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> Vec<ClosedEnvelopeCore> {
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        if !self.route_has_immediate_routing(route) {
            return vec![envelope];
        }
        self.route_envelope(envelope, true)
    }

    pub fn write_single_if_unrouted(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> Option<ClosedEnvelopeCore> {
        if self.route_has_immediate_routing(route) {
            return None;
        }
        Some(self.direct_write(route, payload, producer, control_epoch))
    }

    pub fn write_single_if_unrouted_drop(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> bool {
        if self.route_has_immediate_routing(route) {
            return false;
        }
        self.direct_write_drop(route, payload, producer, control_epoch);
        true
    }

    fn materialize_bytes_from_source_envelope_with_payload_drop(
        &mut self,
        source_envelope: &ClosedEnvelopeCore,
        target_route: &RouteRefCore,
        payload: Vec<u8>,
    ) {
        let target_route = self.register_port(target_route.clone());
        if source_envelope.taints.is_empty() {
            self.direct_write_drop(
                &target_route,
                payload,
                ProducerRefCore {
                    producer_id: "python".to_string(),
                    kind: ProducerKind::Application,
                },
                source_envelope.control_epoch,
            );
            return;
        }
        let mut envelope = self.direct_write(
            &target_route,
            payload,
            ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            },
            source_envelope.control_epoch,
        );
        envelope.taints = source_envelope.taints.clone();
        self.latest.insert(target_route.clone(), envelope.clone());
        if let Some(history) = self.history.get_mut(&target_route) {
            if let Some(retained) = history
                .iter_mut()
                .find(|candidate| candidate.seq_source == envelope.seq_source)
            {
                *retained = envelope;
            }
        }
    }

    pub fn write_single_if_unrouted_and_materializer_drop(
        &mut self,
        route: &RouteRefCore,
        target_route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> bool {
        if self.route_has_immediate_routing(route) {
            return false;
        }
        if !self
            .materialize_targets_by_source
            .get(route)
            .is_some_and(|targets| targets.iter().any(|target| target == target_route))
        {
            return false;
        }
        let shared_payload: Arc<[u8]> = Arc::from(payload);
        if control_epoch.is_none()
            && !Self::route_has_static_taints(route)
            && !Self::route_has_static_taints(target_route)
        {
            self.direct_write_drop_shared_payload(route, shared_payload.clone(), producer, None);
            self.direct_write_drop_shared_payload(
                target_route,
                shared_payload,
                ProducerRefCore {
                    producer_id: "python".to_string(),
                    kind: ProducerKind::Application,
                },
                None,
            );
            return true;
        }
        let target_payload = shared_payload.as_ref().to_vec();
        let payload = shared_payload.as_ref().to_vec();
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        self.materialize_bytes_from_source_envelope_with_payload_drop(
            &envelope,
            target_route,
            target_payload,
        );
        true
    }

    pub fn write_single_if_unrouted_known_materializer_drop(
        &mut self,
        route: &RouteRefCore,
        target_route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
    ) -> bool {
        if self.route_has_immediate_routing(route) {
            return false;
        }
        let shared_payload: Arc<[u8]> = Arc::from(payload);
        if control_epoch.is_none()
            && !Self::route_has_static_taints(route)
            && !Self::route_has_static_taints(target_route)
        {
            self.direct_write_drop_shared_payload(route, shared_payload.clone(), producer, None);
            self.direct_write_drop_shared_payload(
                target_route,
                shared_payload,
                ProducerRefCore {
                    producer_id: "python".to_string(),
                    kind: ProducerKind::Application,
                },
                None,
            );
            return true;
        }
        let target_payload = shared_payload.as_ref().to_vec();
        let payload = shared_payload.as_ref().to_vec();
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        self.materialize_bytes_from_source_envelope_with_payload_drop(
            &envelope,
            target_route,
            target_payload,
        );
        true
    }

    pub fn write_single_if_unrouted_with_lineage_no_parents(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
        trace_id: Option<String>,
        causality_id: Option<String>,
        correlation_id: Option<String>,
    ) -> Option<ClosedEnvelopeCore> {
        if self.route_has_immediate_routing(route) {
            return None;
        }
        let producer_id = Some(producer.producer_id.clone());
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        let (resolved_trace_id, resolved_causality_id) = match (trace_id, causality_id) {
            (Some(trace_id), Some(causality_id)) => (trace_id, causality_id),
            (trace_id, causality_id) => {
                let event_display = format!(
                    "{}@{}",
                    self.registered_route_display(route),
                    envelope.seq_source
                );
                (
                    trace_id.unwrap_or_else(|| event_display.clone()),
                    causality_id.unwrap_or(event_display),
                )
            }
        };
        self.record_lineage_for_new_retained_route_event(
            route,
            envelope.seq_source,
            producer_id,
            resolved_trace_id,
            resolved_causality_id,
            correlation_id,
            Vec::new(),
        );
        Some(envelope)
    }

    pub fn materialize_bytes_one_parent(
        &mut self,
        source_route: &RouteRefCore,
        source_seq_source: u64,
        target_route: &RouteRefCore,
        producer: ProducerRefCore,
    ) -> Option<ClosedEnvelopeCore> {
        let source_route = self.register_port(source_route.clone());
        let target_route = self.register_port(target_route.clone());
        let source_envelope =
            self.retained_envelope_for_route_event(&source_route, source_seq_source)?;
        let payload = self.payload_bytes_for(&source_envelope);
        let source_display = self.registered_route_display(&source_route).to_string();
        let source_event = self
            .optional_route_id(&source_route)
            .map(|route_id| (route_id, source_seq_source));
        let (trace_id, causality_id, correlation_id) = if let Some(lineage) =
            source_event.and_then(|event_key| {
                self.lineage_store()
                    .and_then(|store| store.lineage_by_event.get(&event_key))
            }) {
            (
                self.lineage_value(lineage.trace_id),
                self.lineage_value(lineage.causality_id),
                lineage.correlation_id.clone(),
            )
        } else {
            let event_display = format!("{source_display}@{source_seq_source}");
            (event_display.clone(), event_display, None)
        };
        let parent_events = self.retained_parent_event_list(&source_route, source_seq_source);
        let mut envelope = self.direct_write(
            &target_route,
            payload,
            producer,
            source_envelope.control_epoch,
        );
        if !source_envelope.taints.is_empty() {
            envelope.taints = source_envelope.taints.clone();
            self.latest.insert(target_route.clone(), envelope.clone());
            if let Some(history) = self.history.get_mut(&target_route) {
                if let Some(retained) = history
                    .iter_mut()
                    .find(|candidate| candidate.seq_source == envelope.seq_source)
                {
                    *retained = envelope.clone();
                }
            }
        }
        self.record_lineage_for_new_retained_route_event(
            &target_route,
            envelope.seq_source,
            Some(envelope.producer.producer_id.clone()),
            trace_id,
            causality_id,
            correlation_id,
            parent_events,
        );
        Some(envelope)
    }

    fn materialize_bytes_from_source_envelope(
        &mut self,
        source_envelope: &ClosedEnvelopeCore,
        target_route: &RouteRefCore,
    ) -> ClosedEnvelopeCore {
        let source_route = source_envelope.route.clone();
        let target_route = self.register_port(target_route.clone());
        let payload = self.payload_bytes_for(source_envelope);
        let source_display = self.registered_route_display(&source_route).to_string();
        let source_seq_source = source_envelope.seq_source;
        let source_event = self
            .optional_route_id(&source_route)
            .map(|route_id| (route_id, source_seq_source));
        let (trace_id, causality_id, correlation_id) = if let Some(lineage) =
            source_event.and_then(|event_key| {
                self.lineage_store()
                    .and_then(|store| store.lineage_by_event.get(&event_key))
            }) {
            (
                self.lineage_value(lineage.trace_id),
                self.lineage_value(lineage.causality_id),
                lineage.correlation_id.clone(),
            )
        } else {
            let event_display = format!("{source_display}@{source_seq_source}");
            (event_display.clone(), event_display, None)
        };
        let mut envelope = self.direct_write(
            &target_route,
            payload,
            ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            },
            source_envelope.control_epoch,
        );
        if !source_envelope.taints.is_empty() {
            envelope.taints = source_envelope.taints.clone();
            self.latest.insert(target_route.clone(), envelope.clone());
            if let Some(history) = self.history.get_mut(&target_route) {
                if let Some(retained) = history
                    .iter_mut()
                    .find(|candidate| candidate.seq_source == envelope.seq_source)
                {
                    *retained = envelope.clone();
                }
            }
        }
        self.record_lineage_for_new_retained_route_event(
            &target_route,
            envelope.seq_source,
            Some(envelope.producer.producer_id.clone()),
            trace_id,
            causality_id,
            correlation_id,
            self.retained_parent_event_list(&source_route, source_seq_source),
        );
        envelope
    }

    fn materialize_bytes_from_source_envelope_with_payload_and_lineage_ids(
        &mut self,
        source_envelope: &ClosedEnvelopeCore,
        target_route: &RouteRefCore,
        payload: Vec<u8>,
        trace_id: String,
        causality_id: String,
        correlation_id: Option<String>,
    ) {
        let target_route = self.register_port(target_route.clone());
        let source_seq_source = source_envelope.seq_source;
        if source_envelope.taints.is_empty() {
            let target_seq_source = self.direct_write_drop(
                &target_route,
                payload,
                ProducerRefCore {
                    producer_id: "python".to_string(),
                    kind: ProducerKind::Application,
                },
                source_envelope.control_epoch,
            );
            self.record_lineage_for_new_retained_route_event(
                &target_route,
                target_seq_source,
                Some("python".to_string()),
                trace_id,
                causality_id,
                correlation_id,
                self.retained_parent_event_list(&source_envelope.route, source_seq_source),
            );
            return;
        }
        let mut envelope = self.direct_write(
            &target_route,
            payload,
            ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            },
            source_envelope.control_epoch,
        );
        envelope.taints = source_envelope.taints.clone();
        self.latest.insert(target_route.clone(), envelope.clone());
        if let Some(history) = self.history.get_mut(&target_route) {
            if let Some(retained) = history
                .iter_mut()
                .find(|candidate| candidate.seq_source == envelope.seq_source)
            {
                *retained = envelope.clone();
            }
        }
        self.record_lineage_for_new_retained_route_event(
            &target_route,
            envelope.seq_source,
            Some(envelope.producer.producer_id.clone()),
            trace_id,
            causality_id,
            correlation_id,
            self.retained_parent_event_list(&source_envelope.route, source_seq_source),
        );
    }

    fn write_single_unrouted_untainted_materializer_drop_with_lineage_ids(
        &mut self,
        route: &RouteRefCore,
        target_route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        trace_id: String,
        causality_id: String,
        correlation_id: Option<String>,
    ) -> bool {
        let producer_id = Some(producer.producer_id.clone());
        let shared_payload: Arc<[u8]> = Arc::from(payload);
        let source_seq_source =
            self.direct_write_drop_shared_payload(route, shared_payload.clone(), producer, None);
        self.record_lineage_for_new_retained_route_event(
            route,
            source_seq_source,
            producer_id,
            trace_id.clone(),
            causality_id.clone(),
            correlation_id.clone(),
            Vec::new(),
        );
        let target_seq_source = self.direct_write_drop_shared_payload(
            target_route,
            shared_payload,
            ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            },
            None,
        );
        self.record_lineage_for_new_retained_route_event(
            target_route,
            target_seq_source,
            Some("python".to_string()),
            trace_id,
            causality_id,
            correlation_id,
            self.retained_parent_event_list(route, source_seq_source),
        );
        true
    }

    fn write_single_unrouted_untainted_materializer_drop_with_lineage_profile_ids(
        &mut self,
        route: &RouteRefCore,
        target_route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        trace_id: LineageIdCore,
        causality_id: LineageIdCore,
        correlation_id: Option<String>,
    ) -> bool {
        let producer_id = Some(producer.producer_id.clone());
        let shared_payload: Arc<[u8]> = Arc::from(payload);
        let source_seq_source =
            self.direct_write_drop_shared_payload(route, shared_payload.clone(), producer, None);
        self.record_lineage_for_new_retained_route_event_ids(
            route,
            source_seq_source,
            producer_id,
            trace_id,
            causality_id,
            correlation_id.clone(),
            Vec::new(),
        );
        let target_seq_source = self.direct_write_drop_shared_payload(
            target_route,
            shared_payload,
            ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            },
            None,
        );
        self.record_lineage_for_new_retained_route_event_ids(
            target_route,
            target_seq_source,
            Some("python".to_string()),
            trace_id,
            causality_id,
            correlation_id,
            self.retained_parent_event_list(route, source_seq_source),
        );
        true
    }

    pub fn write_single_if_unrouted_with_lineage_no_parents_and_materializers(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
        trace_id: Option<String>,
        causality_id: Option<String>,
        correlation_id: Option<String>,
    ) -> Option<Vec<ClosedEnvelopeCore>> {
        if self.route_has_immediate_routing(route) {
            return None;
        }
        let producer_id = Some(producer.producer_id.clone());
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        let (resolved_trace_id, resolved_causality_id) = match (trace_id, causality_id) {
            (Some(trace_id), Some(causality_id)) => (trace_id, causality_id),
            (trace_id, causality_id) => {
                let event_display = format!(
                    "{}@{}",
                    self.registered_route_display(route),
                    envelope.seq_source
                );
                (
                    trace_id.unwrap_or_else(|| event_display.clone()),
                    causality_id.unwrap_or(event_display),
                )
            }
        };
        self.record_lineage_for_new_retained_route_event(
            route,
            envelope.seq_source,
            producer_id,
            resolved_trace_id,
            resolved_causality_id,
            correlation_id,
            Vec::new(),
        );
        let mut emitted = vec![envelope.clone()];
        let targets = self
            .materialize_targets_by_source
            .get(route)
            .cloned()
            .unwrap_or_default();
        for target in targets {
            emitted.push(self.materialize_bytes_from_source_envelope(&envelope, &target));
        }
        Some(emitted)
    }

    pub fn write_single_if_unrouted_with_lineage_no_parents_and_materializers_drop(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
        trace_id: Option<String>,
        causality_id: Option<String>,
        correlation_id: Option<String>,
    ) -> bool {
        if self.route_has_immediate_routing(route) {
            return false;
        }
        let producer_id = Some(producer.producer_id.clone());
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        let (resolved_trace_id, resolved_causality_id) = match (trace_id, causality_id) {
            (Some(trace_id), Some(causality_id)) => (trace_id, causality_id),
            (trace_id, causality_id) => {
                let event_display = format!(
                    "{}@{}",
                    self.registered_route_display(route),
                    envelope.seq_source
                );
                (
                    trace_id.unwrap_or_else(|| event_display.clone()),
                    causality_id.unwrap_or(event_display),
                )
            }
        };
        self.record_lineage_for_new_retained_route_event(
            route,
            envelope.seq_source,
            producer_id,
            resolved_trace_id,
            resolved_causality_id,
            correlation_id,
            Vec::new(),
        );
        let targets = self
            .materialize_targets_by_source
            .get(route)
            .cloned()
            .unwrap_or_default();
        for target in targets {
            self.materialize_bytes_from_source_envelope(&envelope, &target);
        }
        true
    }

    pub fn write_single_if_unrouted_with_lineage_ids_no_parents_and_materializers_drop(
        &mut self,
        route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
        trace_id: String,
        causality_id: String,
        correlation_id: Option<String>,
    ) -> bool {
        if self.route_has_immediate_routing(route) {
            return false;
        }
        let producer_id = Some(producer.producer_id.clone());
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        self.record_lineage_for_new_retained_route_event(
            route,
            envelope.seq_source,
            producer_id,
            trace_id,
            causality_id,
            correlation_id,
            Vec::new(),
        );
        let targets = self
            .materialize_targets_by_source
            .get(route)
            .cloned()
            .unwrap_or_default();
        for target in targets {
            self.materialize_bytes_from_source_envelope(&envelope, &target);
        }
        true
    }

    pub fn write_single_if_unrouted_with_lineage_ids_no_parents_and_materializer_drop(
        &mut self,
        route: &RouteRefCore,
        target_route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        control_epoch: Option<u64>,
        trace_id: String,
        causality_id: String,
        correlation_id: Option<String>,
    ) -> bool {
        if self.route_has_immediate_routing(route) {
            return false;
        }
        if !self
            .materialize_targets_by_source
            .get(route)
            .is_some_and(|targets| targets.iter().any(|target| target == target_route))
        {
            return false;
        }
        if control_epoch.is_none()
            && !Self::route_has_static_taints(route)
            && !Self::route_has_static_taints(target_route)
        {
            return self.write_single_unrouted_untainted_materializer_drop_with_lineage_ids(
                route,
                target_route,
                payload,
                producer,
                trace_id,
                causality_id,
                correlation_id,
            );
        }
        let producer_id = Some(producer.producer_id.clone());
        let target_payload = payload.clone();
        let envelope = self.direct_write(route, payload, producer, control_epoch);
        let target_trace_id = trace_id.clone();
        let target_causality_id = causality_id.clone();
        let target_correlation_id = correlation_id.clone();
        self.record_lineage_for_new_retained_route_event(
            route,
            envelope.seq_source,
            producer_id,
            trace_id,
            causality_id,
            correlation_id,
            Vec::new(),
        );
        self.materialize_bytes_from_source_envelope_with_payload_and_lineage_ids(
            &envelope,
            target_route,
            target_payload,
            target_trace_id,
            target_causality_id,
            target_correlation_id,
        );
        true
    }

    pub fn write_single_if_unrouted_with_lineage_profile_no_parents_and_materializer_drop(
        &mut self,
        route: &RouteRefCore,
        target_route: &RouteRefCore,
        payload: Vec<u8>,
        producer: ProducerRefCore,
        trace_id: LineageIdCore,
        causality_id: LineageIdCore,
        correlation_id: Option<String>,
    ) -> bool {
        if !self.lineage_id_is_active(trace_id) || !self.lineage_id_is_active(causality_id) {
            return false;
        }
        if self.route_has_immediate_routing(route) {
            return false;
        }
        if !self
            .materialize_targets_by_source
            .get(route)
            .is_some_and(|targets| targets.iter().any(|target| target == target_route))
        {
            return false;
        }
        if !Self::route_has_static_taints(route) && !Self::route_has_static_taints(target_route) {
            return self
                .write_single_unrouted_untainted_materializer_drop_with_lineage_profile_ids(
                    route,
                    target_route,
                    payload,
                    producer,
                    trace_id,
                    causality_id,
                    correlation_id,
                );
        }
        false
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

    fn lineage_index_count_for_route<K>(
        &self,
        index: &HashMap<K, Vec<EventKeyCore>>,
        route: &RouteRefCore,
    ) -> usize
    where
        K: Eq + Hash,
    {
        let Some(route_id) = self.optional_route_id(route) else {
            return 0;
        };
        index
            .values()
            .flatten()
            .filter(|(event_route_id, _)| *event_route_id == route_id)
            .count()
    }

    fn lineage_index_contains<K>(
        index: &HashMap<K, Vec<EventKeyCore>>,
        lineage_id: &K,
        event_key: &EventKeyCore,
    ) -> bool
    where
        K: Eq + Hash,
    {
        index
            .get(lineage_id)
            .is_some_and(|events| events.iter().filter(|item| *item == event_key).count() == 1)
    }

    pub fn retention_violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        let mut routes = self.descriptors.keys().cloned().collect::<HashSet<_>>();
        routes.extend(self.latest.keys().cloned());
        routes.extend(self.history.keys().cloned());
        routes.extend(self.route_audit.keys().cloned());
        routes.extend(self.retention_policies.keys().cloned());

        let mut retained_payload_ids = HashSet::new();
        let mut retained_event_keys = HashSet::new();
        for route in routes {
            let route_display = self.route_display(&route);
            let route_id = self.optional_route_id(&route);
            let policy = self.retention_policy_for(&route);
            if let Some(history) = self.history.get(&route) {
                if let Some(limit) = Self::native_history_limit(policy.as_ref()) {
                    if history.len() > limit {
                        violations.push(format!(
                            "{route_display} replay retained {} events beyond limit {limit}",
                            history.len()
                        ));
                    }
                }
                retained_payload_ids.extend(
                    history
                        .iter()
                        .map(|envelope| envelope.payload_ref.payload_id.clone()),
                );
                if let Some(route_id) = route_id {
                    retained_event_keys.extend(
                        history
                            .iter()
                            .map(|envelope| (route_id, envelope.seq_source)),
                    );
                }
            }
            if let Some(latest) = self.latest.get(&route) {
                retained_payload_ids.insert(latest.payload_ref.payload_id.clone());
                if let Some(route_id) = route_id {
                    retained_event_keys.insert((route_id, latest.seq_source));
                }
            }
            if let Some(audit) = self.route_audit.get(&route) {
                let oldest_history_seq = self
                    .history
                    .get(&route)
                    .and_then(|history| history.front())
                    .map(|envelope| envelope.seq_source);
                let latest_seq_source = self.latest.get(&route).map(|envelope| envelope.seq_source);
                for event in audit {
                    if let Some(seq_source) = event.seq_source {
                        let retained = Some(seq_source) == latest_seq_source
                            || oldest_history_seq.is_some_and(|oldest| seq_source >= oldest);
                        if !retained {
                            violations.push(format!(
                                "{route_display} audit retained expired seq_source {seq_source}"
                            ));
                        }
                    }
                }
            }
        }

        for payload_id in self.payloads.keys() {
            if !retained_payload_ids.contains(payload_id) {
                violations.push(format!(
                    "payload {payload_id} is not retained by latest/history"
                ));
            }
        }

        if let Some(lineage) = self.lineage_store() {
            for (event_key, record) in &lineage.lineage_by_event {
                if !retained_event_keys.contains(event_key) {
                    violations.push(format!(
                        "lineage {}#{} is not retained by latest/history",
                        self.event_key_display(event_key),
                        event_key.1
                    ));
                }
                if !Self::lineage_index_contains(
                    &lineage.lineage_events_by_trace,
                    &record.trace_id,
                    event_key,
                ) {
                    violations.push(format!(
                        "lineage {}#{} is missing from trace index {}",
                        self.event_key_display(event_key),
                        event_key.1,
                        self.lineage_value(record.trace_id)
                    ));
                }
                if !Self::lineage_index_contains(
                    &lineage.lineage_events_by_causality,
                    &record.causality_id,
                    event_key,
                ) {
                    violations.push(format!(
                        "lineage {}#{} is missing from causality index {}",
                        self.event_key_display(event_key),
                        event_key.1,
                        self.lineage_value(record.causality_id)
                    ));
                }
                if let Some(correlation_id) = &record.correlation_id {
                    if !Self::lineage_index_contains(
                        &lineage.lineage_events_by_correlation,
                        correlation_id,
                        event_key,
                    ) {
                        violations.push(format!(
                            "lineage {}#{} is missing from correlation index {correlation_id}",
                            self.event_key_display(event_key),
                            event_key.1
                        ));
                    }
                }
            }
            self.lineage_index_violations(
                "trace",
                &lineage.lineage_events_by_trace,
                &lineage.lineage_by_event,
                &mut violations,
                GraphCore::lineage_id_label,
            );
            self.lineage_index_violations(
                "causality",
                &lineage.lineage_events_by_causality,
                &lineage.lineage_by_event,
                &mut violations,
                GraphCore::lineage_id_label,
            );
            self.lineage_index_violations(
                "correlation",
                &lineage.lineage_events_by_correlation,
                &lineage.lineage_by_event,
                &mut violations,
                GraphCore::lineage_string_label,
            );
        }

        violations.sort();
        violations
    }

    fn lineage_id_label(&self, lineage_id: &LineageIdCore) -> String {
        self.lineage_value(*lineage_id)
    }

    #[allow(clippy::ptr_arg)]
    fn lineage_string_label(&self, lineage_id: &String) -> String {
        lineage_id.clone()
    }

    fn lineage_index_violations<K>(
        &self,
        index_name: &str,
        index: &HashMap<K, Vec<EventKeyCore>>,
        lineage: &HashMap<EventKeyCore, RetainedLineageCore>,
        violations: &mut Vec<String>,
        lineage_label: fn(&Self, &K) -> String,
    ) where
        K: Eq + Hash,
    {
        for (lineage_id, events) in index {
            let mut seen = HashSet::new();
            for event_key in events {
                let duplicate = !seen.insert(event_key);
                let missing = !lineage.contains_key(event_key);
                if duplicate || missing {
                    let lineage_value = lineage_label(self, lineage_id);
                    let event_route_display = self.event_key_display(event_key);
                    if duplicate {
                        violations.push(format!(
                            "{index_name} index {lineage_value} duplicates {}#{}",
                            event_route_display, event_key.1
                        ));
                    }
                    if missing {
                        violations.push(format!(
                            "{index_name} index {lineage_value} references missing lineage {}#{}",
                            event_route_display, event_key.1
                        ));
                    }
                }
            }
        }
    }

    pub fn retention_snapshot(&self, route: &RouteRefCore) -> RetentionSnapshotCore {
        let route_display = self.route_display(route);
        let policy = self.retention_policy_for(route);
        RetentionSnapshotCore {
            route_display,
            latest_seq_source: self.latest.get(route).map(|envelope| envelope.seq_source),
            metadata_event_count: self
                .latest
                .get(route)
                .map_or(0, |envelope| envelope.seq_source),
            replay_count: self.history.get(route).map_or(0, VecDeque::len),
            payload_count: self.retained_payload_count(route),
            lineage_count: self
                .lineage_store()
                .and_then(|lineage| {
                    lineage.route_ids.get(route).map(|route_id| {
                        lineage
                            .lineage_by_event
                            .keys()
                            .filter(|(event_route_id, _)| event_route_id == route_id)
                            .count()
                    })
                })
                .unwrap_or(0),
            trace_index_count: self.lineage_store().map_or(0, |lineage| {
                self.lineage_index_count_for_route(&lineage.lineage_events_by_trace, route)
            }),
            causality_index_count: self.lineage_store().map_or(0, |lineage| {
                self.lineage_index_count_for_route(&lineage.lineage_events_by_causality, route)
            }),
            correlation_index_count: self.lineage_store().map_or(0, |lineage| {
                self.lineage_index_count_for_route(&lineage.lineage_events_by_correlation, route)
            }),
            history_limit: Self::native_history_limit(policy.as_ref()),
        }
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
        Some(OpenedEnvelopeCore {
            closed,
            payload: payload.as_ref().to_vec(),
        })
    }

    pub fn record_lineage(&mut self, record: LineageRecordCore) {
        let Some(route) = self.routes_by_display.get(&record.event_route_display) else {
            return;
        };
        let route = route.clone();
        self.record_lineage_for_route(
            &route,
            record.event_seq_source,
            record.producer_id,
            record.trace_id,
            record.causality_id,
            record.correlation_id,
            record.parent_events,
        );
    }

    pub fn record_lineage_for_route(
        &mut self,
        route: &RouteRefCore,
        seq_source: u64,
        producer_id: Option<String>,
        trace_id: String,
        causality_id: String,
        correlation_id: Option<String>,
        parent_events: Vec<(String, u64)>,
    ) {
        if !self
            .lineage_store()
            .is_some_and(|lineage| lineage.lineage_retained_routes.contains(route))
        {
            return;
        }
        if !self.route_retains_event(route, seq_source) {
            return;
        }
        let event_key = self.retained_event_key(route, seq_source);
        if self
            .lineage_store()
            .is_some_and(|lineage| lineage.lineage_by_event.contains_key(&event_key))
        {
            self.forget_lineage_event(&event_key);
        }
        let correlation_id = self.retained_correlation_id(correlation_id);
        let trace_id = self.intern_lineage_id(&trace_id);
        let causality_id = self.intern_lineage_id(&causality_id);
        self.retain_lineage_id(trace_id);
        self.retain_lineage_id(causality_id);
        let parent_events = self.retained_parent_events_from_display(parent_events);
        let lineage = self.ensure_lineage_store();
        lineage
            .lineage_events_by_trace
            .entry(trace_id)
            .or_default()
            .push(event_key);
        lineage
            .lineage_events_by_causality
            .entry(causality_id)
            .or_default()
            .push(event_key);
        if let Some(correlation_id) = &correlation_id {
            lineage
                .lineage_events_by_correlation
                .entry(correlation_id.clone())
                .or_default()
                .push(event_key);
        }
        lineage.lineage_by_event.insert(
            event_key,
            RetainedLineageCore {
                producer_id,
                trace_id,
                causality_id,
                correlation_id,
                parent_events,
            },
        );
    }

    fn record_lineage_for_new_retained_route_event(
        &mut self,
        route: &RouteRefCore,
        seq_source: u64,
        producer_id: Option<String>,
        trace_id: String,
        causality_id: String,
        correlation_id: Option<String>,
        parent_events: Vec<RetainedParentEventCore>,
    ) {
        if !self
            .lineage_store()
            .is_some_and(|lineage| lineage.lineage_retained_routes.contains(route))
        {
            return;
        }
        let correlation_id = self.retained_correlation_id(correlation_id);
        let trace_id = self.intern_lineage_id(&trace_id);
        let causality_id = self.intern_lineage_id(&causality_id);
        self.record_lineage_for_new_retained_route_event_ids(
            route,
            seq_source,
            producer_id,
            trace_id,
            causality_id,
            correlation_id,
            parent_events,
        );
    }

    fn record_lineage_for_new_retained_route_event_ids(
        &mut self,
        route: &RouteRefCore,
        seq_source: u64,
        producer_id: Option<String>,
        trace_id: LineageIdCore,
        causality_id: LineageIdCore,
        correlation_id: Option<String>,
        parent_events: Vec<RetainedParentEventCore>,
    ) {
        if !self
            .lineage_store()
            .is_some_and(|lineage| lineage.lineage_retained_routes.contains(route))
        {
            return;
        }
        let correlation_id = self.retained_correlation_id(correlation_id);
        let event_key = self.retained_event_key(route, seq_source);
        self.retain_lineage_id(trace_id);
        self.retain_lineage_id(causality_id);
        let lineage = self.ensure_lineage_store();
        lineage
            .lineage_events_by_trace
            .entry(trace_id)
            .or_default()
            .push(event_key);
        lineage
            .lineage_events_by_causality
            .entry(causality_id)
            .or_default()
            .push(event_key);
        if let Some(correlation_id) = &correlation_id {
            lineage
                .lineage_events_by_correlation
                .entry(correlation_id.clone())
                .or_default()
                .push(event_key);
        }
        lineage.lineage_by_event.insert(
            event_key,
            RetainedLineageCore {
                producer_id,
                trace_id,
                causality_id,
                correlation_id,
                parent_events,
            },
        );
    }

    pub fn lineage_records(
        &self,
        route: Option<&RouteRefCore>,
        trace_id: Option<&str>,
        causality_id: Option<&str>,
        correlation_id: Option<&str>,
    ) -> Vec<LineageRecordCore> {
        let keys = if let Some(trace_id) = trace_id {
            self.lineage_id_for_value(trace_id)
                .and_then(|lineage_id| {
                    self.lineage_store()
                        .and_then(|lineage| lineage.lineage_events_by_trace.get(&lineage_id))
                        .cloned()
                })
                .unwrap_or_default()
        } else if let Some(causality_id) = causality_id {
            self.lineage_id_for_value(causality_id)
                .and_then(|lineage_id| {
                    self.lineage_store()
                        .and_then(|lineage| lineage.lineage_events_by_causality.get(&lineage_id))
                        .cloned()
                })
                .unwrap_or_default()
        } else if let Some(correlation_id) = correlation_id {
            self.lineage_store()
                .and_then(|lineage| lineage.lineage_events_by_correlation.get(correlation_id))
                .cloned()
                .unwrap_or_default()
        } else if let Some(route) = route {
            self.retained_event_keys_for_route(route)
        } else {
            let Some(lineage) = self.lineage_store() else {
                return Vec::new();
            };
            let mut keyed_records = lineage
                .lineage_by_event
                .keys()
                .map(|key| (self.event_key_display(key), key.1, *key))
                .collect::<Vec<_>>();
            keyed_records
                .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
            keyed_records
                .into_iter()
                .map(|(_, _, key)| key)
                .collect::<Vec<_>>()
        };
        let route_display = route.map(|route| self.route_display(route));
        keys.into_iter()
            .filter_map(|key| {
                let record = self.lineage_store()?.lineage_by_event.get(&key)?;
                let display = self.event_key_display(&key);
                if route_display
                    .as_ref()
                    .is_some_and(|route_display| route_display != &display)
                {
                    return None;
                }
                Some(LineageRecordCore {
                    event_route_display: display,
                    event_seq_source: key.1,
                    producer_id: record.producer_id.clone(),
                    trace_id: self.lineage_value(record.trace_id),
                    causality_id: self.lineage_value(record.causality_id),
                    correlation_id: record.correlation_id.clone(),
                    parent_events: self.retained_parent_events_to_display(&record.parent_events),
                })
            })
            .collect()
    }

    pub fn lineage_record_for_route_event(
        &self,
        route: &RouteRefCore,
        seq_source: u64,
    ) -> Option<LineageRecordCore> {
        let event_key = (self.optional_route_id(route)?, seq_source);
        let record = self.lineage_store()?.lineage_by_event.get(&event_key)?;
        Some(LineageRecordCore {
            event_route_display: self.route_display(route),
            event_seq_source: seq_source,
            producer_id: record.producer_id.clone(),
            trace_id: self.lineage_value(record.trace_id),
            causality_id: self.lineage_value(record.causality_id),
            correlation_id: record.correlation_id.clone(),
            parent_events: self.retained_parent_events_to_display(&record.parent_events),
        })
    }

    pub fn retained_payload_count(&self, route: &RouteRefCore) -> usize {
        let mut retained = HashSet::new();
        if let Some(latest) = self.latest.get(route) {
            retained.insert(latest.payload_ref.payload_id.as_str());
        }
        if let Some(history) = self.history.get(route) {
            for envelope in history {
                retained.insert(envelope.payload_ref.payload_id.as_str());
            }
        }
        retained
            .into_iter()
            .filter(|payload_id| self.payloads.contains_key(*payload_id))
            .count()
    }

    pub fn payload_by_id(&self, payload_id: &str) -> Option<Vec<u8>> {
        self.payloads
            .get(payload_id)
            .map(|payload| payload.as_ref().to_vec())
    }

    pub fn query(&self, query: QueryKindCore) -> QueryResultCore {
        match query {
            QueryKindCore::Catalog => {
                let mut routes = self.descriptors.keys().cloned().collect::<Vec<_>>();
                routes.sort_by_cached_key(|route| self.route_display(route));
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
                    .map(|(left, right)| (self.route_display(left), self.route_display(right)))
                    .collect::<Vec<_>>();
                edges.sort();
                QueryResultCore::Topology(edges)
            }
            QueryKindCore::Trace => {
                let mut items = self.latest.values().cloned().collect::<Vec<_>>();
                items.sort_by_cached_key(|envelope| {
                    (envelope.seq_source, self.route_display(&envelope.route))
                });
                QueryResultCore::Trace(items)
            }
            QueryKindCore::Replay(route) => QueryResultCore::Replay(
                self.history
                    .get(&route)
                    .map(|history| history.iter().cloned().collect())
                    .unwrap_or_default(),
            ),
            QueryKindCore::ValidateGraph => {
                let mut issues = Vec::new();
                let mut schema_mismatches = self
                    .edges
                    .iter()
                    .filter(|(source, sink)| source.schema != sink.schema)
                    .map(|(source, sink)| {
                        let source_display = self.route_display(source);
                        let sink_display = self.route_display(sink);
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
                let mut mailbox_issues = self
                    .mailboxes
                    .iter()
                    .filter(|(_, mailbox)| mailbox.descriptor.capacity == 0)
                    .map(|(name, _)| format!("Mailbox {name} must declare positive capacity"))
                    .collect::<Vec<_>>();
                mailbox_issues.sort();
                for issue in mailbox_issues {
                    issues.push(issue);
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
        assert!(first[0].trace_id.is_none());
        assert!(first[0].causality_id.is_none());
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
        assert_eq!(
            graph.outgoing_edges_by_source.get(&source),
            Some(&vec![sink.clone()])
        );
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
    fn disconnect_clears_core_fanout_index() {
        let source = sample_route(
            Plane::Read,
            Layer::Logical,
            "sensor",
            "telemetry",
            "disconnect_source",
            Variant::Event,
        );
        let sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "sensor",
            "telemetry",
            "disconnect_sink",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "app".to_string(),
            kind: ProducerKind::Application,
        };

        assert!(graph.connect(&source, &sink));
        assert!(graph.disconnect(&source, &sink));
        assert!(!graph.disconnect(&source, &sink));
        assert!(!graph.outgoing_edges_by_source.contains_key(&source));

        let emitted = graph
            .write_single_if_unrouted(&source, b"sample".to_vec(), producer, None)
            .expect("disconnected source should use no-fanout write path");

        assert_eq!(emitted.route, source);
        assert!(!graph.latest.contains_key(&sink));
    }

    #[test]
    fn routed_route_cache_tracks_topology_mutations() {
        let source = sample_route(
            Plane::Read,
            Layer::Logical,
            "sensor",
            "telemetry",
            "cache_source",
            Variant::Event,
        );
        let sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "sensor",
            "telemetry",
            "cache_sink",
            Variant::Event,
        );
        let mailbox_ingress = sample_route(
            Plane::Write,
            Layer::Internal,
            "mailbox",
            "mailbox",
            "cache_mailbox",
            Variant::Request,
        );
        let mailbox_egress = sample_route(
            Plane::Read,
            Layer::Internal,
            "mailbox",
            "mailbox",
            "cache_mailbox",
            Variant::Meta,
        );
        let request = sample_route(
            Plane::Write,
            Layer::Logical,
            "counter",
            "counter",
            "cache_request",
            Variant::Request,
        );
        let desired = sample_route(
            Plane::Write,
            Layer::Shadow,
            "counter",
            "counter",
            "cache_request",
            Variant::Desired,
        );
        let reported = sample_route(
            Plane::Write,
            Layer::Shadow,
            "counter",
            "counter",
            "cache_request",
            Variant::Reported,
        );
        let effective = sample_route(
            Plane::Write,
            Layer::Shadow,
            "counter",
            "counter",
            "cache_request",
            Variant::Effective,
        );
        let mut graph = GraphCore::default();

        graph.register_port(source.clone());
        assert!(!graph.routed_routes.contains(&source));
        assert!(graph.connect(&source, &sink));
        assert!(graph.routed_routes.contains(&source));
        assert!(graph.disconnect(&source, &sink));
        assert!(!graph.routed_routes.contains(&source));

        graph.register_mailbox(
            "cache_mailbox".to_string(),
            MailboxCore {
                ingress: mailbox_ingress.clone(),
                egress: mailbox_egress,
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
        assert!(graph.routed_routes.contains(&mailbox_ingress));

        graph.register_binding(
            "counter".to_string(),
            WriteBindingCore {
                request: request.clone(),
                desired,
                reported,
                effective,
                ack: None,
            },
        );
        assert!(graph.routed_routes.contains(&request));
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
        assert_eq!(graph.mailbox_egress_by_ingress.get(&ingress), Some(&egress));
        assert_eq!(
            graph.mailbox_name_by_egress.get(&egress),
            Some(&"work".to_string())
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

        let emitted = graph.write(&route, b"point-cloud".to_vec(), producer, None);
        assert_eq!(emitted.len(), 1);
        let closed = emitted
            .into_iter()
            .next()
            .expect("write should emit a closed envelope");
        assert!(closed.payload_ref.inline_bytes.is_empty());
        assert_eq!(graph.retained_payload_count(&route), 1);
        assert!(graph.retention_violations().is_empty());

        let opened = graph
            .open_latest(&route)
            .expect("payload should open from store");
        assert_eq!(opened.payload, b"point-cloud".to_vec());
        assert_eq!(graph.route_audit.get(&route).expect("audit trail").len(), 2);
    }

    #[test]
    fn single_write_fast_path_refuses_routed_routes() {
        let source = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "source",
            Variant::Event,
        );
        let sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "sink",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        graph.connect(&source, &sink);
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };

        assert!(graph
            .write_single_if_unrouted(&source, b"frame".to_vec(), producer.clone(), None)
            .is_none());
        let emitted = graph.write(&source, b"frame".to_vec(), producer, None);
        assert_eq!(emitted.len(), 2);
        assert!(graph.latest.contains_key(&sink));
    }

    #[test]
    fn single_write_drop_fast_path_keeps_bounded_native_retention() {
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "drop_fast_path",
            Variant::Event,
        );
        let sink = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "drop_fast_path_sink",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };
        graph.configure_retention(
            &route,
            RetentionPolicyCore {
                latest_replay_policy: "bounded_history".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "last_2".to_string(),
                payload_retention_policy: "separate_store".to_string(),
                history_limit: Some(2),
                lineage_retention_policy: "none".to_string(),
            },
        );

        for value in 0..10 {
            assert!(graph.write_single_if_unrouted_drop(
                &route,
                format!("frame-{value}").into_bytes(),
                producer.clone(),
                None,
            ));
        }

        let replay = graph.query(QueryKindCore::Replay(route.clone()));
        let QueryResultCore::Replay(history) = replay else {
            panic!("unexpected query result");
        };
        assert_eq!(history.len(), 2);
        assert_eq!(
            history
                .iter()
                .map(|envelope| envelope.seq_source)
                .collect::<Vec<_>>(),
            vec![9, 10]
        );
        assert_eq!(
            graph.latest.get(&route).map(|envelope| envelope.seq_source),
            Some(10)
        );
        assert_eq!(graph.retained_payload_count(&route), 2);
        assert!(graph.retention_violations().is_empty());

        graph.connect(&route, &sink);
        assert!(!graph.write_single_if_unrouted_drop(
            &route,
            b"routed-frame".to_vec(),
            producer,
            None,
        ));
        assert_eq!(
            graph.latest.get(&route).map(|envelope| envelope.seq_source),
            Some(10)
        );
    }

    #[test]
    fn latest_only_routes_bound_native_history_payloads_and_audit() {
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "imu",
            "sensor",
            "accel",
            Variant::Meta,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "imu".to_string(),
            kind: ProducerKind::Device,
        };
        graph.configure_retention(
            &route,
            RetentionPolicyCore {
                latest_replay_policy: "latest_only".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "latest".to_string(),
                payload_retention_policy: "separate_store".to_string(),
                history_limit: Some(1),
                lineage_retention_policy: "none".to_string(),
            },
        );

        for value in 0..10 {
            graph.write(
                &route,
                format!("sample-{value}").into_bytes(),
                producer.clone(),
                None,
            );
        }

        let history = graph.history.get(&route).expect("retained history");
        let audit = graph.route_audit.get(&route).expect("retained audit");

        assert_eq!(history.len(), 1);
        assert_eq!(history[0].seq_source, 10);
        assert_eq!(
            history[0].payload_ref.payload_id,
            event_payload_id(&route.display(), 10)
        );
        assert_eq!(graph.payloads.len(), 1);
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].seq_source, Some(10));
        assert_eq!(
            graph
                .open_latest(&route)
                .expect("latest payload should remain")
                .payload,
            b"sample-9".to_vec()
        );
    }

    #[test]
    fn native_payload_open_audit_is_coalesced_and_retained() {
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "payload_open_audit",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        graph.attach_retained_lineage_store();
        graph.configure_retention(
            &route,
            RetentionPolicyCore {
                latest_replay_policy: "bounded_history".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "last_2".to_string(),
                payload_retention_policy: "separate_store".to_string(),
                history_limit: Some(2),
                lineage_retention_policy: "none".to_string(),
            },
        );
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };

        graph.write(&route, b"first".to_vec(), producer.clone(), None);
        for _ in 0..100 {
            assert!(graph.open_latest(&route).is_some());
        }
        let audit = graph.route_audit.get(&route).expect("retained audit");
        assert_eq!(audit.len(), 2);
        assert_eq!(
            audit
                .iter()
                .filter(|event| event.action == "payload_open")
                .count(),
            1
        );

        for value in 0..10 {
            graph.write(
                &route,
                format!("sample-{value}").into_bytes(),
                producer.clone(),
                None,
            );
            assert!(graph.open_latest(&route).is_some());
        }
        let audit = graph.route_audit.get(&route).expect("retained audit");
        assert_eq!(audit.len(), 2);
        assert_eq!(
            audit
                .iter()
                .map(|event| (event.action.as_str(), event.seq_source))
                .collect::<Vec<_>>(),
            vec![("write", Some(11)), ("payload_open", Some(11))]
        );
        assert!(graph.retention_violations().is_empty());
    }

    #[test]
    fn default_routes_bound_native_history_payloads_without_lineage() {
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "default_bounded",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };

        for value in 0..(DEFAULT_ROUTE_HISTORY_LIMIT + 5) {
            let emitted = graph.write(
                &route,
                format!("sample-{value}").into_bytes(),
                producer.clone(),
                None,
            );
            graph.record_lineage(LineageRecordCore {
                event_route_display: route.display(),
                event_seq_source: emitted[0].seq_source,
                producer_id: Some("heart".to_string()),
                trace_id: "default-trace".to_string(),
                causality_id: "default-chain".to_string(),
                correlation_id: Some(format!("request-{value}")),
                parent_events: Vec::new(),
            });
        }

        assert_eq!(
            graph.history.get(&route).map_or(0, VecDeque::len),
            DEFAULT_ROUTE_HISTORY_LIMIT
        );
        assert_eq!(
            graph.retained_payload_count(&route),
            DEFAULT_ROUTE_HISTORY_LIMIT
        );
        assert_eq!(graph.retained_lineage_event_count(), 0);
        assert!(graph
            .lineage_records(None, None, None, Some("request-0"))
            .is_empty());
    }

    #[test]
    fn bounded_history_without_limit_uses_default_native_bound() {
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "implicit_bounded",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        graph.configure_retention(
            &route,
            RetentionPolicyCore {
                latest_replay_policy: "bounded_history".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "memory".to_string(),
                payload_retention_policy: "separate_store".to_string(),
                history_limit: None,
                lineage_retention_policy: "none".to_string(),
            },
        );
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };

        for value in 0..(DEFAULT_ROUTE_HISTORY_LIMIT + 2) {
            graph.write(
                &route,
                format!("sample-{value}").into_bytes(),
                producer.clone(),
                None,
            );
        }

        assert_eq!(
            graph.history.get(&route).map_or(0, VecDeque::len),
            DEFAULT_ROUTE_HISTORY_LIMIT
        );
        assert_eq!(
            graph.retained_payload_count(&route),
            DEFAULT_ROUTE_HISTORY_LIMIT
        );
    }

    #[test]
    fn retained_payload_count_tracks_each_route_lifecycle() {
        let first = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "payload_index_first",
            Variant::Event,
        );
        let second = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "payload_index_second",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        for route in [&first, &second] {
            graph.configure_retention(
                route,
                RetentionPolicyCore {
                    latest_replay_policy: "bounded_history".to_string(),
                    durability_class: "memory".to_string(),
                    replay_window: "last_2".to_string(),
                    payload_retention_policy: "separate_store".to_string(),
                    history_limit: Some(2),
                    lineage_retention_policy: "none".to_string(),
                },
            );
        }
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };

        for value in 0..50 {
            graph.write(
                &first,
                format!("first-{value}").into_bytes(),
                producer.clone(),
                None,
            );
            graph.write(
                &second,
                format!("second-{value}").into_bytes(),
                producer.clone(),
                None,
            );
        }

        assert_eq!(graph.retained_payload_count(&first), 2);
        assert_eq!(graph.retained_payload_count(&second), 2);
        assert_eq!(graph.payloads.len(), 4);
        assert!(graph.retention_violations().is_empty());
    }

    #[test]
    fn non_replayable_routes_keep_latest_payload_without_history_growth() {
        let route = sample_route(
            Plane::Read,
            Layer::Ephemeral,
            "session",
            "trace",
            "entropy",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "session".to_string(),
            kind: ProducerKind::Application,
        };

        for value in 0..10 {
            graph.write(
                &route,
                format!("nonce-{value}").into_bytes(),
                producer.clone(),
                None,
            );
        }

        assert_eq!(graph.history.get(&route).map_or(0, VecDeque::len), 0);
        assert_eq!(graph.payloads.len(), 1);
        assert_eq!(
            graph
                .open_latest(&route)
                .expect("latest payload should remain")
                .payload,
            b"nonce-9".to_vec()
        );
    }

    #[test]
    fn native_lineage_ignores_unknown_or_expired_events() {
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "lineage_lifetime",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        graph.attach_retained_lineage_store();
        graph.configure_retention(
            &route,
            RetentionPolicyCore {
                latest_replay_policy: "latest_only".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "latest".to_string(),
                payload_retention_policy: "separate_store".to_string(),
                history_limit: Some(1),
                lineage_retention_policy: "retained".to_string(),
            },
        );
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };
        graph.record_lineage(LineageRecordCore {
            event_route_display: "unknown:route".to_string(),
            event_seq_source: 1,
            producer_id: Some("heart".to_string()),
            trace_id: "orphan-trace".to_string(),
            causality_id: "orphan-chain".to_string(),
            correlation_id: Some("orphan-request".to_string()),
            parent_events: Vec::new(),
        });

        let first = graph.write(&route, b"first".to_vec(), producer.clone(), None);
        let second = graph.write(&route, b"second".to_vec(), producer.clone(), None);
        graph.record_lineage(LineageRecordCore {
            event_route_display: route.display(),
            event_seq_source: first[0].seq_source,
            producer_id: Some("heart".to_string()),
            trace_id: "expired-trace".to_string(),
            causality_id: "expired-chain".to_string(),
            correlation_id: Some("expired-request".to_string()),
            parent_events: Vec::new(),
        });
        graph.record_lineage(LineageRecordCore {
            event_route_display: route.display(),
            event_seq_source: second[0].seq_source,
            producer_id: Some("heart".to_string()),
            trace_id: "latest-trace".to_string(),
            causality_id: "latest-chain".to_string(),
            correlation_id: Some("latest-request".to_string()),
            parent_events: Vec::new(),
        });

        assert!(graph
            .lineage_records(None, Some("orphan-trace"), None, None)
            .is_empty());
        assert!(graph
            .lineage_records(None, Some("expired-trace"), None, None)
            .is_empty());
        assert_eq!(
            graph
                .lineage_records(Some(&route), None, None, None)
                .iter()
                .map(|record| record.trace_id.as_str())
                .collect::<Vec<_>>(),
            vec!["latest-trace"]
        );
        assert_eq!(graph.retained_lineage_event_count(), 1);
        assert!(graph.retention_violations().is_empty());
    }

    #[test]
    fn retention_violations_report_orphan_payloads_and_lineage_indexes() {
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "retention_violation",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        graph.attach_retained_lineage_store();
        graph.register_port(route.clone());
        graph
            .payloads
            .insert("orphan-payload".to_string(), Arc::from(&b"leaked"[..]));
        let route_id = graph.registered_route_id(&route);
        let orphan_trace_id = graph.intern_lineage_id("orphan-trace");
        graph
            .lineage
            .as_mut()
            .expect("retained lineage should be attached")
            .lineage_events_by_trace
            .entry(orphan_trace_id)
            .or_default()
            .push((route_id, 42));

        let violations = graph.retention_violations();

        assert!(violations
            .iter()
            .any(|violation| violation.contains("payload orphan-payload")));
        assert!(violations
            .iter()
            .any(|violation| violation
                .contains("trace index orphan-trace references missing lineage")));
    }

    #[test]
    fn lineage_indexes_follow_native_route_retention() {
        let route = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "lineage",
            Variant::Event,
        );
        let mut graph = GraphCore::default();
        graph.attach_retained_lineage_store();
        graph.attach_retained_correlation_store();
        graph.configure_retention(
            &route,
            RetentionPolicyCore {
                latest_replay_policy: "bounded_history".to_string(),
                durability_class: "memory".to_string(),
                replay_window: "last_2".to_string(),
                payload_retention_policy: "separate_store".to_string(),
                history_limit: Some(2),
                lineage_retention_policy: "retained".to_string(),
            },
        );
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };

        for value in 0..10 {
            let emitted = graph.write(
                &route,
                format!("sample-{value}").into_bytes(),
                producer.clone(),
                None,
            );
            graph.record_lineage(LineageRecordCore {
                event_route_display: route.display(),
                event_seq_source: emitted[0].seq_source,
                producer_id: Some("heart".to_string()),
                trace_id: "runtime-trace".to_string(),
                causality_id: "runtime-chain".to_string(),
                correlation_id: Some(format!("request-{value}")),
                parent_events: Vec::new(),
            });
        }

        let trace_records = graph.lineage_records(None, Some("runtime-trace"), None, None);
        let expired_records = graph.lineage_records(None, None, None, Some("request-0"));

        assert_eq!(
            trace_records
                .iter()
                .map(|record| record.event_seq_source)
                .collect::<Vec<_>>(),
            vec![9, 10]
        );
        assert!(expired_records.is_empty());
        assert_eq!(graph.retained_lineage_event_count(), 2);
        let runtime_trace_id = graph
            .lineage_id_for_value("runtime-trace")
            .expect("runtime trace id should be interned");
        let lineage = graph
            .lineage
            .as_ref()
            .expect("retained lineage should be attached");
        assert_eq!(lineage.lineage_events_by_trace[&runtime_trace_id].len(), 2);
        let route_id = graph.registered_route_id(&route);
        assert_eq!(
            lineage.lineage_events_by_trace[&runtime_trace_id],
            vec![(route_id, 9), (route_id, 10)]
        );
    }

    #[test]
    fn explicit_materializer_drop_path_bounds_source_and_state_retention() {
        let source = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "materialize_drop_source",
            Variant::Event,
        );
        let state = sample_route(
            Plane::State,
            Layer::Logical,
            "heart",
            "runtime",
            "materialize_drop_state",
            Variant::State,
        );
        let mut graph = GraphCore::default();
        graph.attach_retained_lineage_store();
        for route in [&source, &state] {
            graph.configure_retention(
                route,
                RetentionPolicyCore {
                    latest_replay_policy: "bounded_history".to_string(),
                    durability_class: "memory".to_string(),
                    replay_window: "last_2".to_string(),
                    payload_retention_policy: "separate_store".to_string(),
                    history_limit: Some(2),
                    lineage_retention_policy: "retained".to_string(),
                },
            );
        }
        assert!(graph.register_materialize_bytes(&source, &state));
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };

        for value in 0..10 {
            assert!(graph
                .write_single_if_unrouted_with_lineage_ids_no_parents_and_materializer_drop(
                    &source,
                    &state,
                    format!("frame-{value}").into_bytes(),
                    producer.clone(),
                    None,
                    "runtime-trace".to_string(),
                    "runtime-chain".to_string(),
                    Some(format!("request-{value}")),
                ));
        }

        for route in [&source, &state] {
            let QueryResultCore::Replay(history) =
                graph.query(QueryKindCore::Replay(route.clone()))
            else {
                panic!("unexpected query result");
            };
            assert_eq!(
                history
                    .iter()
                    .map(|envelope| envelope.seq_source)
                    .collect::<Vec<_>>(),
                vec![9, 10]
            );
            assert_eq!(graph.retained_payload_count(route), 2);
            assert_eq!(graph.retention_snapshot(route).lineage_count, 2);
        }
        let source_latest = graph.latest.get(&source).expect("source latest retained");
        let state_latest = graph.latest.get(&state).expect("state latest retained");
        assert_ne!(
            source_latest.payload_ref.payload_id,
            state_latest.payload_ref.payload_id
        );
        let source_payload = graph
            .payloads
            .get(&source_latest.payload_ref.payload_id)
            .expect("source payload retained");
        let state_payload = graph
            .payloads
            .get(&state_latest.payload_ref.payload_id)
            .expect("state payload retained");
        assert!(Arc::ptr_eq(source_payload, state_payload));
        assert!(graph
            .lineage_records(None, None, None, Some("request-0"))
            .is_empty());
        let lineage = graph
            .lineage
            .as_ref()
            .expect("retained lineage should be attached");
        let state_key = (
            *lineage
                .route_ids
                .get(&state)
                .expect("state route id should be registered"),
            10,
        );
        let source_route_id = *lineage
            .route_ids
            .get(&source)
            .expect("source route id should be registered");
        assert_eq!(
            lineage
                .lineage_by_event
                .get(&state_key)
                .expect("latest state lineage should be retained natively")
                .parent_events,
            vec![RetainedParentEventCore::RouteId(source_route_id, 10)]
        );
        let latest_state_lineage = graph.lineage_records(Some(&state), None, None, None);
        assert_eq!(latest_state_lineage.len(), 2);
        let latest_state_record = latest_state_lineage
            .iter()
            .find(|record| record.event_seq_source == 10)
            .expect("latest state lineage should be retained");
        assert_eq!(
            latest_state_record.parent_events,
            vec![(source.display(), 10)]
        );
        assert!(graph.retention_violations().is_empty());
    }

    #[test]
    fn materializer_generation_invalidates_known_pair_fast_path() {
        let source = sample_route(
            Plane::Read,
            Layer::Logical,
            "heart",
            "runtime",
            "materialize_generation_source",
            Variant::Event,
        );
        let state = sample_route(
            Plane::State,
            Layer::Logical,
            "heart",
            "runtime",
            "materialize_generation_state",
            Variant::State,
        );
        let mut graph = GraphCore::default();
        let producer = ProducerRefCore {
            producer_id: "heart".to_string(),
            kind: ProducerKind::Application,
        };

        assert_eq!(graph.materialize_generation, 0);
        assert!(graph.register_materialize_bytes(&source, &state));
        let compiled_generation = graph.materialize_generation;
        assert_eq!(compiled_generation, 1);
        assert!(!graph.register_materialize_bytes(&source, &state));
        assert_eq!(graph.materialize_generation, compiled_generation);

        assert!(graph.write_single_if_unrouted_known_materializer_drop(
            &source,
            &state,
            b"frame".to_vec(),
            producer.clone(),
            None,
        ));
        let state_payload_id = graph.latest[&state].payload_ref.payload_id.clone();
        assert_eq!(
            graph.payloads[&state_payload_id].as_ref(),
            b"frame".as_slice()
        );
        assert!(graph.unregister_materialize_bytes(&source, &state));
        assert!(graph.materialize_generation > compiled_generation);
        assert!(!graph.write_single_if_unrouted_and_materializer_drop(
            &source,
            &state,
            b"stale".to_vec(),
            producer,
            None,
        ));
        assert_eq!(
            graph.latest[&state].payload_ref.payload_id,
            state_payload_id
        );
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
