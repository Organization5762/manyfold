#![allow(clippy::too_many_arguments)]

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::architecture::{
    InMemoryPubSubCore, PubSubDeliveryCore, PubSubMessageCore, PubSubSubscriptionCore,
};
use crate::core::{
    ClockDomainRefCore, ClosedEnvelopeCore, ControlLoopCore, CreditSnapshotCore, DeliveryMode,
    GraphCore, Layer, MailboxCore, MailboxDescriptorCore, NamespaceRefCore, OpenedEnvelopeCore,
    OrderingPolicy, OverflowPolicy, Plane, PortDescriptorCore, ProducerKind, ProducerRefCore,
    QueryKindCore, QueryResultCore, RetentionPolicyCore, RetentionSnapshotCore, RouteRefCore,
    RuntimeRefCore, ScheduleConditionCore, ScheduleGuardCore, SchemaRefCore, TaintDomain,
    TaintMarkCore, Variant, WriteBindingCore,
};
use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList};
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, ToSql};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::define_stub_info_gatherer;

const DELIVERY_MODES: &[&str] = &[
    "mpsc_serial",
    "mpmc_unique",
    "mpmc_replicated",
    "key_affine",
];
const ORDERING_POLICIES: &[&str] = &[
    "fifo",
    "priority_stable",
    "weighted_fair",
    "round_robin_by_producer",
    "keyed_fifo",
    "latest_only",
    "unordered",
];
const OVERFLOW_POLICIES: &[&str] = &[
    "block",
    "drop_oldest",
    "drop_newest",
    "coalesce_latest",
    "deadline_drop",
    "spill_to_store",
    "reject_write",
];

fn lock_graph(state: &Arc<Mutex<GraphCore>>) -> PyResult<std::sync::MutexGuard<'_, GraphCore>> {
    state
        .lock()
        .map_err(|_| PyRuntimeError::new_err("graph mutex poisoned"))
}

fn validate_route(route: &RouteRefCore) -> PyResult<()> {
    let valid = match route.namespace.plane {
        Plane::Read => matches!(
            route.variant,
            Variant::Meta | Variant::Payload | Variant::State | Variant::Event | Variant::Health
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
        return Err(PyValueError::new_err(format!(
            "variant {} is not valid for plane {}",
            route.variant.as_str(),
            route.namespace.plane.as_str()
        )));
    }
    if route.namespace.plane != Plane::Write && route.namespace.layer == Layer::Shadow {
        return Err(PyValueError::new_err(
            "shadow routes are only valid under the write plane",
        ));
    }
    if route.namespace.plane == Plane::Write
        && route.variant == Variant::Request
        && route.namespace.layer == Layer::Shadow
    {
        return Err(PyValueError::new_err(
            "write request routes must not use the shadow layer",
        ));
    }
    Ok(())
}

fn unsupported_choice(field: &str, value: &str, choices: &[&str]) -> PyErr {
    PyValueError::new_err(format!(
        "unsupported {field} {value:?}; expected one of {}",
        choices.join(", ")
    ))
}

fn validate_nonblank_text(field: &str, value: &str) -> PyResult<()> {
    if value.trim().is_empty() {
        return Err(PyValueError::new_err(format!(
            "{field} must be a non-empty string"
        )));
    }
    Ok(())
}

fn invalid_non_negative_integer(field: &str) -> PyErr {
    PyValueError::new_err(format!("{field} must be a non-negative integer"))
}

fn invalid_positive_integer(field: &str) -> PyErr {
    PyValueError::new_err(format!("{field} must be a positive integer"))
}

fn extract_nonbool_u64(value: &Bound<'_, PyAny>, field: &str) -> PyResult<u64> {
    if value.is_instance_of::<PyBool>() {
        return Err(invalid_non_negative_integer(field));
    }
    value
        .extract::<u64>()
        .map_err(|_| invalid_non_negative_integer(field))
}

fn extract_positive_u32(value: &Bound<'_, PyAny>, field: &str) -> PyResult<u32> {
    if value.is_instance_of::<PyBool>() {
        return Err(invalid_positive_integer(field));
    }
    let parsed = value
        .extract::<u32>()
        .map_err(|_| invalid_positive_integer(field))?;
    if parsed == 0 {
        return Err(invalid_positive_integer(field));
    }
    Ok(parsed)
}

fn extract_positive_capacity(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "capacity must be an integer, not bool",
        ));
    }
    let capacity = value.extract::<usize>()?;
    if capacity == 0 {
        return Err(PyValueError::new_err("capacity must be greater than zero"));
    }
    Ok(capacity)
}

fn extract_logical_length_bytes(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    extract_nonbool_u64(value, "logical_length_bytes")
}

fn current_system_time_ns() -> PyResult<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            PyRuntimeError::new_err(format!("system clock before Unix epoch: {error}"))
        })?;
    u64::try_from(duration.as_nanos())
        .map_err(|_| PyRuntimeError::new_err("system time does not fit in nanoseconds"))
}

fn extract_optional_control_epoch(value: &Bound<'_, PyAny>) -> PyResult<Option<u64>> {
    if value.is_none() {
        return Ok(None);
    }
    if value.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err(
            "control_epoch must be a non-negative integer or None",
        ));
    }
    value
        .extract::<u64>()
        .map(Some)
        .map_err(|_| PyValueError::new_err("control_epoch must be a non-negative integer or None"))
}

fn extract_schedule_epoch(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    extract_nonbool_u64(value, "epoch")
}

fn extract_schema_version(value: &Bound<'_, PyAny>) -> PyResult<u32> {
    extract_positive_u32(value, "schema version")
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(
    eq,
    frozen,
    module = "manyfold._manyfold_rust",
    name = "Plane",
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PyPlane {
    inner: Plane,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
#[allow(non_snake_case)]
impl PyPlane {
    #[classattr]
    fn Read() -> PyPlane {
        Self { inner: Plane::Read }
    }
    #[classattr]
    fn Write() -> PyPlane {
        Self {
            inner: Plane::Write,
        }
    }
    #[classattr]
    fn State() -> PyPlane {
        Self {
            inner: Plane::State,
        }
    }
    #[classattr]
    fn Query() -> PyPlane {
        Self {
            inner: Plane::Query,
        }
    }
    #[classattr]
    fn Debug() -> PyPlane {
        Self {
            inner: Plane::Debug,
        }
    }
    #[getter]
    fn value(&self) -> &'static str {
        self.inner.as_str()
    }
    fn __repr__(&self) -> String {
        format!("Plane.{}", self.inner.as_str().to_uppercase())
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(
    eq,
    frozen,
    module = "manyfold._manyfold_rust",
    name = "Layer",
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PyLayer {
    inner: Layer,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
#[allow(non_snake_case)]
impl PyLayer {
    #[classattr]
    fn Raw() -> PyLayer {
        Self { inner: Layer::Raw }
    }
    #[classattr]
    fn Logical() -> PyLayer {
        Self {
            inner: Layer::Logical,
        }
    }
    #[classattr]
    fn Shadow() -> PyLayer {
        Self {
            inner: Layer::Shadow,
        }
    }
    #[classattr]
    fn Bulk() -> PyLayer {
        Self { inner: Layer::Bulk }
    }
    #[classattr]
    fn Internal() -> PyLayer {
        Self {
            inner: Layer::Internal,
        }
    }
    #[classattr]
    fn Ephemeral() -> PyLayer {
        Self {
            inner: Layer::Ephemeral,
        }
    }
    #[getter]
    fn value(&self) -> &'static str {
        self.inner.as_str()
    }
    fn __repr__(&self) -> String {
        format!("Layer.{}", self.inner.as_str().to_uppercase())
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(
    eq,
    frozen,
    module = "manyfold._manyfold_rust",
    name = "Variant",
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PyVariant {
    inner: Variant,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
#[allow(non_snake_case)]
impl PyVariant {
    #[classattr]
    fn Meta() -> PyVariant {
        Self {
            inner: Variant::Meta,
        }
    }
    #[classattr]
    fn Payload() -> PyVariant {
        Self {
            inner: Variant::Payload,
        }
    }
    #[classattr]
    fn Request() -> PyVariant {
        Self {
            inner: Variant::Request,
        }
    }
    #[classattr]
    fn Desired() -> PyVariant {
        Self {
            inner: Variant::Desired,
        }
    }
    #[classattr]
    fn Reported() -> PyVariant {
        Self {
            inner: Variant::Reported,
        }
    }
    #[classattr]
    fn Effective() -> PyVariant {
        Self {
            inner: Variant::Effective,
        }
    }
    #[classattr]
    fn Ack() -> PyVariant {
        Self {
            inner: Variant::Ack,
        }
    }
    #[classattr]
    fn State() -> PyVariant {
        Self {
            inner: Variant::State,
        }
    }
    #[classattr]
    fn QueryRequest() -> PyVariant {
        Self {
            inner: Variant::QueryRequest,
        }
    }
    #[classattr]
    fn QueryResponse() -> PyVariant {
        Self {
            inner: Variant::QueryResponse,
        }
    }
    #[classattr]
    fn Event() -> PyVariant {
        Self {
            inner: Variant::Event,
        }
    }
    #[classattr]
    fn Health() -> PyVariant {
        Self {
            inner: Variant::Health,
        }
    }
    #[getter]
    fn value(&self) -> &'static str {
        self.inner.as_str()
    }
    fn __repr__(&self) -> String {
        format!("Variant.{}", self.inner.as_str().to_uppercase())
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(
    eq,
    frozen,
    module = "manyfold._manyfold_rust",
    name = "ProducerKind",
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PyProducerKind {
    inner: ProducerKind,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
#[allow(non_snake_case)]
impl PyProducerKind {
    #[classattr]
    fn Device() -> PyProducerKind {
        Self {
            inner: ProducerKind::Device,
        }
    }
    #[classattr]
    fn FirmwareAgent() -> PyProducerKind {
        Self {
            inner: ProducerKind::FirmwareAgent,
        }
    }
    #[classattr]
    fn Transform() -> PyProducerKind {
        Self {
            inner: ProducerKind::Transform,
        }
    }
    #[classattr]
    fn ControlLoop() -> PyProducerKind {
        Self {
            inner: ProducerKind::ControlLoop,
        }
    }
    #[classattr]
    fn Mailbox() -> PyProducerKind {
        Self {
            inner: ProducerKind::Mailbox,
        }
    }
    #[classattr]
    fn QueryService() -> PyProducerKind {
        Self {
            inner: ProducerKind::QueryService,
        }
    }
    #[classattr]
    fn Application() -> PyProducerKind {
        Self {
            inner: ProducerKind::Application,
        }
    }
    #[classattr]
    fn Bridge() -> PyProducerKind {
        Self {
            inner: ProducerKind::Bridge,
        }
    }
    #[classattr]
    fn Reconciler() -> PyProducerKind {
        Self {
            inner: ProducerKind::Reconciler,
        }
    }
    #[classattr]
    fn LifecycleService() -> PyProducerKind {
        Self {
            inner: ProducerKind::LifecycleService,
        }
    }
    #[getter]
    fn value(&self) -> &'static str {
        self.inner.as_str()
    }
    fn __repr__(&self) -> String {
        format!("ProducerKind.{}", self.inner.as_str().to_uppercase())
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(
    eq,
    frozen,
    module = "manyfold._manyfold_rust",
    name = "TaintDomain",
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PyTaintDomain {
    inner: TaintDomain,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
#[allow(non_snake_case)]
impl PyTaintDomain {
    #[classattr]
    fn Time() -> PyTaintDomain {
        Self {
            inner: TaintDomain::Time,
        }
    }
    #[classattr]
    fn Order() -> PyTaintDomain {
        Self {
            inner: TaintDomain::Order,
        }
    }
    #[classattr]
    fn Delivery() -> PyTaintDomain {
        Self {
            inner: TaintDomain::Delivery,
        }
    }
    #[classattr]
    fn Determinism() -> PyTaintDomain {
        Self {
            inner: TaintDomain::Determinism,
        }
    }
    #[classattr]
    fn Scheduling() -> PyTaintDomain {
        Self {
            inner: TaintDomain::Scheduling,
        }
    }
    #[classattr]
    fn Trust() -> PyTaintDomain {
        Self {
            inner: TaintDomain::Trust,
        }
    }
    #[classattr]
    fn Coherence() -> PyTaintDomain {
        Self {
            inner: TaintDomain::Coherence,
        }
    }
    #[getter]
    fn value(&self) -> &'static str {
        self.inner.as_str()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct NamespaceRef {
    pub(crate) inner: NamespaceRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl NamespaceRef {
    #[new]
    fn new(plane: PyPlane, layer: PyLayer, owner: String) -> PyResult<Self> {
        validate_nonblank_text("owner", &owner)?;
        Ok(Self {
            inner: NamespaceRefCore {
                plane: plane.inner,
                layer: layer.inner,
                owner,
            },
        })
    }
    #[getter]
    fn plane(&self) -> PyPlane {
        PyPlane {
            inner: self.inner.plane,
        }
    }
    #[getter]
    fn layer(&self) -> PyLayer {
        PyLayer {
            inner: self.inner.layer,
        }
    }
    #[getter]
    fn owner(&self) -> String {
        self.inner.owner.clone()
    }
    fn __repr__(&self) -> String {
        format!(
            "NamespaceRef(plane='{}', layer='{}', owner='{}')",
            self.inner.plane.as_str(),
            self.inner.layer.as_str(),
            self.inner.owner
        )
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct SchemaRef {
    pub(crate) inner: SchemaRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl SchemaRef {
    #[new]
    fn new(
        schema_id: String,
        #[pyo3(from_py_with = extract_schema_version)] version: u32,
    ) -> PyResult<Self> {
        validate_nonblank_text("schema_id", &schema_id)?;
        Ok(Self {
            inner: SchemaRefCore { schema_id, version },
        })
    }
    #[getter]
    fn schema_id(&self) -> String {
        self.inner.schema_id.clone()
    }
    #[getter]
    fn version(&self) -> u32 {
        self.inner.version
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct RouteRef {
    pub(crate) inner: RouteRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl RouteRef {
    #[new]
    fn new(
        namespace: Py<NamespaceRef>,
        family: String,
        stream: String,
        variant: PyVariant,
        schema: Py<SchemaRef>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        validate_nonblank_text("family", &family)?;
        validate_nonblank_text("stream", &stream)?;
        let inner = RouteRefCore {
            namespace: namespace.borrow(py).inner.clone(),
            family,
            stream,
            variant: variant.inner,
            schema: schema.borrow(py).inner.clone(),
        };
        validate_route(&inner)?;
        Ok(Self { inner })
    }
    #[getter]
    fn namespace(&self) -> NamespaceRef {
        NamespaceRef {
            inner: self.inner.namespace.clone(),
        }
    }
    #[getter]
    fn family(&self) -> String {
        self.inner.family.clone()
    }
    #[getter]
    fn stream(&self) -> String {
        self.inner.stream.clone()
    }
    #[getter]
    fn variant(&self) -> PyVariant {
        PyVariant {
            inner: self.inner.variant,
        }
    }
    #[getter]
    fn schema(&self) -> SchemaRef {
        SchemaRef {
            inner: self.inner.schema.clone(),
        }
    }
    fn display(&self) -> String {
        self.inner.display()
    }
    fn __richcmp__(&self, other: &Bound<'_, PyAny>, op: CompareOp) -> PyResult<bool> {
        let other_route = other.extract::<PyRef<'_, RouteRef>>();
        match op {
            CompareOp::Eq => Ok(other_route
                .as_ref()
                .is_ok_and(|route| self.inner == route.inner)),
            CompareOp::Ne => Ok(other_route
                .as_ref()
                .map_or(true, |route| self.inner != route.inner)),
            _ => Err(PyTypeError::new_err("RouteRef does not support ordering")),
        }
    }
    fn __hash__(&self) -> isize {
        let mut hasher = DefaultHasher::new();
        self.inner.hash(&mut hasher);
        let value = hasher.finish() as isize;
        if value == -1 {
            -2
        } else {
            value
        }
    }
    fn __repr__(&self) -> String {
        format!("RouteRef({})", self.inner.display())
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct ProducerRef {
    inner: ProducerRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ProducerRef {
    #[new]
    fn new(producer_id: String, kind: PyProducerKind) -> PyResult<Self> {
        validate_nonblank_text("producer_id", &producer_id)?;
        Ok(Self {
            inner: ProducerRefCore {
                producer_id,
                kind: kind.inner,
            },
        })
    }
    #[getter]
    fn producer_id(&self) -> String {
        self.inner.producer_id.clone()
    }
    #[getter]
    fn kind(&self) -> PyProducerKind {
        PyProducerKind {
            inner: self.inner.kind,
        }
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct RuntimeRef {
    inner: RuntimeRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl RuntimeRef {
    #[new]
    fn new(runtime_id: String) -> PyResult<Self> {
        validate_nonblank_text("runtime_id", &runtime_id)?;
        Ok(Self {
            inner: RuntimeRefCore { runtime_id },
        })
    }
    #[getter]
    fn runtime_id(&self) -> String {
        self.inner.runtime_id.clone()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct ClockDomainRef {
    inner: ClockDomainRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ClockDomainRef {
    #[new]
    fn new(clock_domain_id: String) -> PyResult<Self> {
        validate_nonblank_text("clock_domain_id", &clock_domain_id)?;
        Ok(Self {
            inner: ClockDomainRefCore { clock_domain_id },
        })
    }
    #[getter]
    fn clock_domain_id(&self) -> String {
        self.inner.clock_domain_id.clone()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct PayloadRef {
    inner: crate::core::PayloadRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl PayloadRef {
    #[new]
    #[pyo3(signature = (payload_id, logical_length_bytes=0, codec_id="identity".to_string(), inline_bytes=Vec::new()))]
    fn new(
        payload_id: String,
        #[pyo3(from_py_with = extract_logical_length_bytes)] logical_length_bytes: u64,
        codec_id: String,
        inline_bytes: Vec<u8>,
    ) -> PyResult<Self> {
        validate_nonblank_text("payload_id", &payload_id)?;
        validate_nonblank_text("codec_id", &codec_id)?;
        Ok(Self {
            inner: crate::core::PayloadRefCore {
                payload_id,
                logical_length_bytes,
                codec_id,
                inline_bytes,
            },
        })
    }
    #[getter]
    fn payload_id(&self) -> String {
        self.inner.payload_id.clone()
    }
    #[getter]
    fn logical_length_bytes(&self) -> u64 {
        self.inner.logical_length_bytes
    }
    #[getter]
    fn codec_id(&self) -> String {
        self.inner.codec_id.clone()
    }
    #[getter]
    fn inline_bytes(&self) -> Vec<u8> {
        self.inner.inline_bytes.clone()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct TaintMark {
    inner: TaintMarkCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl TaintMark {
    #[new]
    fn new(domain: PyTaintDomain, value_id: String, origin_id: String) -> PyResult<Self> {
        validate_nonblank_text("value_id", &value_id)?;
        validate_nonblank_text("origin_id", &origin_id)?;
        Ok(Self {
            inner: TaintMarkCore {
                domain: domain.inner,
                value_id,
                origin_id,
            },
        })
    }
    #[getter]
    fn domain(&self) -> PyTaintDomain {
        PyTaintDomain {
            inner: self.inner.domain,
        }
    }
    #[getter]
    fn value_id(&self) -> String {
        self.inner.value_id.clone()
    }
    #[getter]
    fn origin_id(&self) -> String {
        self.inner.origin_id.clone()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct ScheduleGuard {
    inner: ScheduleGuardCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ScheduleGuard {
    #[staticmethod]
    fn not_before_epoch(#[pyo3(from_py_with = extract_schedule_epoch)] epoch: u64) -> Self {
        Self {
            inner: ScheduleGuardCore {
                condition: ScheduleConditionCore::NotBeforeEpoch(epoch),
                expires_at_epoch: None,
            },
        }
    }
    #[staticmethod]
    fn wait_for_ack(route: RouteRef) -> Self {
        Self {
            inner: ScheduleGuardCore {
                condition: ScheduleConditionCore::WaitForAckRoute(route.inner),
                expires_at_epoch: None,
            },
        }
    }
    #[getter]
    fn expires_at_epoch(&self) -> Option<u64> {
        self.inner.expires_at_epoch
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct ClosedEnvelope {
    inner: ClosedEnvelopeCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ClosedEnvelope {
    #[getter]
    fn route(&self) -> RouteRef {
        RouteRef {
            inner: self.inner.route.clone(),
        }
    }
    #[getter]
    fn producer(&self) -> ProducerRef {
        ProducerRef {
            inner: self.inner.producer.clone(),
        }
    }
    #[getter]
    fn emitter(&self) -> RuntimeRef {
        RuntimeRef {
            inner: self.inner.emitter.clone(),
        }
    }
    #[getter]
    fn seq_source(&self) -> u64 {
        self.inner.seq_source
    }
    #[getter]
    fn control_epoch(&self) -> Option<u64> {
        self.inner.control_epoch
    }
    #[getter]
    fn taints(&self) -> Vec<TaintMark> {
        self.inner
            .taints
            .iter()
            .cloned()
            .map(|inner| TaintMark { inner })
            .collect()
    }
    #[getter]
    fn guards(&self) -> Vec<ScheduleGuard> {
        self.inner
            .guards
            .iter()
            .cloned()
            .map(|inner| ScheduleGuard { inner })
            .collect()
    }
    #[getter]
    fn payload_ref(&self) -> PayloadRef {
        PayloadRef {
            inner: self.inner.payload_ref.clone(),
        }
    }
    #[getter]
    fn payload_id(&self) -> String {
        self.inner.payload_ref.payload_id.clone()
    }
    #[getter]
    fn has_inline_payload(&self) -> bool {
        !self.inner.payload_ref.inline_bytes.is_empty()
    }
    #[getter]
    fn inline_payload(&self) -> Vec<u8> {
        self.inner.payload_ref.inline_bytes.clone()
    }
    fn with_taints(&self, taints: Vec<TaintMark>) -> Self {
        let mut inner = self.inner.clone();
        inner.taints = taints.into_iter().map(|taint| taint.inner).collect();
        Self { inner }
    }
    fn close(&self) -> Self {
        self.clone()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct OpenedEnvelope {
    inner: OpenedEnvelopeCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl OpenedEnvelope {
    #[getter]
    fn closed(&self) -> ClosedEnvelope {
        ClosedEnvelope {
            inner: self.inner.closed.clone(),
        }
    }
    #[getter]
    fn payload(&self) -> Vec<u8> {
        self.inner.payload.clone()
    }
    fn close(&self) -> ClosedEnvelope {
        ClosedEnvelope {
            inner: self.inner.closed.clone(),
        }
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct PortDescriptor {
    inner: PortDescriptorCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl PortDescriptor {
    #[getter]
    fn route_display(&self) -> String {
        self.inner.identity.route_ref.display()
    }
    #[getter]
    fn human_description(&self) -> String {
        self.inner.identity.human_description.clone()
    }
    #[getter]
    fn payload_open_policy(&self) -> String {
        self.inner.schema.payload_open_policy.clone()
    }
    #[getter]
    fn backpressure_policy(&self) -> String {
        self.inner.flow.backpressure_policy.clone()
    }
    #[getter]
    fn debug_enabled(&self) -> bool {
        self.inner.debug.audit_enabled
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", from_py_object)]
#[derive(Clone)]
pub struct ReadablePort {
    graph: Arc<Mutex<GraphCore>>,
    route: RouteRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ReadablePort {
    fn meta(&self) -> PyResult<Vec<ClosedEnvelope>> {
        let graph = lock_graph(&self.graph)?;
        Ok(graph
            .latest
            .get(&self.route)
            .cloned()
            .into_iter()
            .map(|inner| ClosedEnvelope { inner })
            .collect())
    }
    fn open(&self) -> PyResult<Vec<OpenedEnvelope>> {
        let mut graph = lock_graph(&self.graph)?;
        Ok(graph
            .open_latest(&self.route)
            .into_iter()
            .map(|inner| OpenedEnvelope { inner })
            .collect())
    }
    fn latest(&self) -> PyResult<Option<ClosedEnvelope>> {
        let graph = lock_graph(&self.graph)?;
        Ok(graph
            .latest
            .get(&self.route)
            .cloned()
            .map(|inner| ClosedEnvelope { inner }))
    }
    fn describe(&self) -> PyResult<PortDescriptor> {
        let graph = lock_graph(&self.graph)?;
        let descriptor = graph
            .descriptors
            .get(&self.route)
            .cloned()
            .unwrap_or_else(|| PortDescriptorCore::for_route(&self.route));
        Ok(PortDescriptor { inner: descriptor })
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", from_py_object)]
#[derive(Clone)]
pub struct WritablePort {
    graph: Arc<Mutex<GraphCore>>,
    route: RouteRefCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl WritablePort {
    #[pyo3(signature = (payload, producer=None, control_epoch=None))]
    fn write(
        &self,
        payload: Vec<u8>,
        producer: Option<ProducerRef>,
        #[pyo3(from_py_with = extract_optional_control_epoch)] control_epoch: Option<u64>,
    ) -> PyResult<ClosedEnvelope> {
        let mut graph = lock_graph(&self.graph)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        let inner = graph
            .write(&self.route, payload, producer, control_epoch)
            .into_iter()
            .next()
            .ok_or_else(|| PyRuntimeError::new_err("write emitted no envelopes"))?;
        Ok(ClosedEnvelope { inner })
    }
    fn describe(&self) -> PyResult<PortDescriptor> {
        let graph = lock_graph(&self.graph)?;
        let descriptor = graph
            .descriptors
            .get(&self.route)
            .cloned()
            .unwrap_or_else(|| PortDescriptorCore::for_route(&self.route));
        Ok(PortDescriptor { inner: descriptor })
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct WriteBinding {
    inner: WriteBindingCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl WriteBinding {
    #[new]
    #[pyo3(signature = (request, desired, reported, effective, ack=None))]
    fn new(
        request: RouteRef,
        desired: RouteRef,
        reported: RouteRef,
        effective: RouteRef,
        ack: Option<RouteRef>,
    ) -> PyResult<Self> {
        if request.inner.variant != Variant::Request {
            return Err(PyValueError::new_err(
                "request route must use Variant.Request",
            ));
        }
        let inner = WriteBindingCore {
            request: request.inner,
            desired: desired.inner,
            reported: reported.inner,
            effective: effective.inner,
            ack: ack.map(|route| route.inner),
        };
        inner.validate().map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }
    #[getter]
    fn request(&self) -> RouteRef {
        RouteRef {
            inner: self.inner.request.clone(),
        }
    }
    #[getter]
    fn desired(&self) -> RouteRef {
        RouteRef {
            inner: self.inner.desired.clone(),
        }
    }
    #[getter]
    fn reported(&self) -> RouteRef {
        RouteRef {
            inner: self.inner.reported.clone(),
        }
    }
    #[getter]
    fn effective(&self) -> RouteRef {
        RouteRef {
            inner: self.inner.effective.clone(),
        }
    }
    #[getter]
    fn ack(&self) -> Option<RouteRef> {
        self.inner.ack.clone().map(|inner| RouteRef { inner })
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct MailboxDescriptor {
    inner: MailboxDescriptorCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl MailboxDescriptor {
    #[new]
    #[pyo3(signature = (capacity=128, delivery_mode="mpsc_serial".to_string(), ordering_policy="fifo".to_string(), overflow_policy="block".to_string()))]
    fn new(
        #[pyo3(from_py_with = extract_positive_capacity)] capacity: usize,
        delivery_mode: String,
        ordering_policy: String,
        overflow_policy: String,
    ) -> PyResult<Self> {
        let delivery_mode = match delivery_mode.as_str() {
            "mpsc_serial" => DeliveryMode::MpscSerial,
            "mpmc_unique" => DeliveryMode::MpmcUnique,
            "mpmc_replicated" => DeliveryMode::MpmcReplicated,
            "key_affine" => DeliveryMode::KeyAffine,
            value => return Err(unsupported_choice("delivery_mode", value, DELIVERY_MODES)),
        };
        let ordering_policy = match ordering_policy.as_str() {
            "fifo" => OrderingPolicy::Fifo,
            "priority_stable" => OrderingPolicy::PriorityStable,
            "weighted_fair" => OrderingPolicy::WeightedFair,
            "round_robin_by_producer" => OrderingPolicy::RoundRobinByProducer,
            "keyed_fifo" => OrderingPolicy::KeyedFifo,
            "latest_only" => OrderingPolicy::LatestOnly,
            "unordered" => OrderingPolicy::Unordered,
            value => {
                return Err(unsupported_choice(
                    "ordering_policy",
                    value,
                    ORDERING_POLICIES,
                ));
            }
        };
        let overflow_policy = match overflow_policy.as_str() {
            "block" => OverflowPolicy::Block,
            "drop_oldest" => OverflowPolicy::DropOldest,
            "drop_newest" => OverflowPolicy::DropNewest,
            "coalesce_latest" => OverflowPolicy::CoalesceLatest,
            "deadline_drop" => OverflowPolicy::DeadlineDrop,
            "spill_to_store" => OverflowPolicy::SpillToStore,
            "reject_write" => OverflowPolicy::RejectWrite,
            value => {
                return Err(unsupported_choice(
                    "overflow_policy",
                    value,
                    OVERFLOW_POLICIES,
                ))
            }
        };
        Ok(Self {
            inner: MailboxDescriptorCore {
                delivery_mode,
                ordering_policy,
                overflow_policy,
                capacity,
            },
        })
    }
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity
    }

    #[getter]
    fn delivery_mode(&self) -> String {
        match self.inner.delivery_mode {
            DeliveryMode::MpscSerial => "mpsc_serial",
            DeliveryMode::MpmcUnique => "mpmc_unique",
            DeliveryMode::MpmcReplicated => "mpmc_replicated",
            DeliveryMode::KeyAffine => "key_affine",
        }
        .to_string()
    }

    #[getter]
    fn ordering_policy(&self) -> String {
        match self.inner.ordering_policy {
            OrderingPolicy::Fifo => "fifo",
            OrderingPolicy::PriorityStable => "priority_stable",
            OrderingPolicy::WeightedFair => "weighted_fair",
            OrderingPolicy::RoundRobinByProducer => "round_robin_by_producer",
            OrderingPolicy::KeyedFifo => "keyed_fifo",
            OrderingPolicy::LatestOnly => "latest_only",
            OrderingPolicy::Unordered => "unordered",
        }
        .to_string()
    }

    #[getter]
    fn overflow_policy(&self) -> String {
        match self.inner.overflow_policy {
            OverflowPolicy::Block => "block",
            OverflowPolicy::DropOldest => "drop_oldest",
            OverflowPolicy::DropNewest => "drop_newest",
            OverflowPolicy::CoalesceLatest => "coalesce_latest",
            OverflowPolicy::DeadlineDrop => "deadline_drop",
            OverflowPolicy::SpillToStore => "spill_to_store",
            OverflowPolicy::RejectWrite => "reject_write",
        }
        .to_string()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct CreditSnapshot {
    inner: CreditSnapshotCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl CreditSnapshot {
    #[getter]
    fn route_display(&self) -> String {
        self.inner.route_display.clone()
    }

    #[getter]
    fn credit_class(&self) -> String {
        self.inner.credit_class.clone()
    }

    #[getter]
    fn available(&self) -> u64 {
        self.inner.available
    }

    #[getter]
    fn blocked_senders(&self) -> u64 {
        self.inner.blocked_senders
    }

    #[getter]
    fn dropped_messages(&self) -> u64 {
        self.inner.dropped_messages
    }

    #[getter]
    fn largest_queue_depth(&self) -> usize {
        self.inner.largest_queue_depth
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct RetentionSnapshot {
    inner: RetentionSnapshotCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct NoLineageMaterializerDropProfile {
    source_route: RouteRefCore,
    target_route: RouteRefCore,
    materialize_generation: u64,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl RetentionSnapshot {
    #[getter]
    fn route_display(&self) -> String {
        self.inner.route_display.clone()
    }

    #[getter]
    fn latest_seq_source(&self) -> Option<u64> {
        self.inner.latest_seq_source
    }

    #[getter]
    fn metadata_event_count(&self) -> u64 {
        self.inner.metadata_event_count
    }

    #[getter]
    fn replay_count(&self) -> usize {
        self.inner.replay_count
    }

    #[getter]
    fn payload_count(&self) -> usize {
        self.inner.payload_count
    }

    #[getter]
    fn lineage_count(&self) -> usize {
        self.inner.lineage_count
    }

    #[getter]
    fn trace_index_count(&self) -> usize {
        self.inner.trace_index_count
    }

    #[getter]
    fn causality_index_count(&self) -> usize {
        self.inner.causality_index_count
    }

    #[getter]
    fn correlation_index_count(&self) -> usize {
        self.inner.correlation_index_count
    }

    #[getter]
    fn history_limit(&self) -> Option<usize> {
        self.inner.history_limit
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", from_py_object)]
#[derive(Clone)]
pub struct Mailbox {
    graph: Arc<Mutex<GraphCore>>,
    name: String,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl Mailbox {
    #[getter]
    fn ingress(&self) -> PyResult<WritablePort> {
        let graph = lock_graph(&self.graph)?;
        let mailbox = graph
            .mailboxes
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?;
        Ok(WritablePort {
            graph: Arc::clone(&self.graph),
            route: mailbox.ingress.clone(),
        })
    }
    #[getter]
    fn egress(&self) -> PyResult<ReadablePort> {
        let graph = lock_graph(&self.graph)?;
        let mailbox = graph
            .mailboxes
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?;
        Ok(ReadablePort {
            graph: Arc::clone(&self.graph),
            route: mailbox.egress.clone(),
        })
    }
    fn name(&self) -> String {
        self.name.clone()
    }

    fn depth(&self) -> PyResult<usize> {
        let graph = lock_graph(&self.graph)?;
        let mailbox = graph
            .mailboxes
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?;
        Ok(mailbox.queue.len())
    }

    fn available_credit(&self) -> PyResult<usize> {
        let graph = lock_graph(&self.graph)?;
        let mailbox = graph
            .mailboxes
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?;
        Ok(mailbox
            .descriptor
            .capacity
            .saturating_sub(mailbox.queue.len()))
    }

    fn blocked_writes(&self) -> PyResult<u64> {
        let graph = lock_graph(&self.graph)?;
        let mailbox = graph
            .mailboxes
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?;
        Ok(mailbox.blocked_writes)
    }

    fn dropped_messages(&self) -> PyResult<u64> {
        let graph = lock_graph(&self.graph)?;
        let mailbox = graph
            .mailboxes
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?;
        Ok(mailbox.dropped_messages)
    }

    fn coalesced_messages(&self) -> PyResult<u64> {
        let graph = lock_graph(&self.graph)?;
        let mailbox = graph
            .mailboxes
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?;
        Ok(mailbox.coalesced_messages)
    }

    fn delivered_messages(&self) -> PyResult<u64> {
        let graph = lock_graph(&self.graph)?;
        let mailbox = graph
            .mailboxes
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?;
        Ok(mailbox.delivered_messages)
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct ControlLoop {
    inner: ControlLoopCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ControlLoop {
    #[new]
    fn new(name: String, read_routes: Vec<RouteRef>, write_request: RouteRef) -> PyResult<Self> {
        validate_nonblank_text("control loop name", &name)?;
        Ok(Self {
            inner: ControlLoopCore {
                name,
                read_routes: read_routes.into_iter().map(|route| route.inner).collect(),
                write_route: write_request.inner,
                epoch: 0,
            },
        })
    }
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }
    #[getter]
    fn epoch(&self) -> u64 {
        self.inner.epoch
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust")]
pub struct Graph {
    state: Arc<Mutex<GraphCore>>,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl Graph {
    #[new]
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(GraphCore::default())),
        }
    }

    fn register_port(&self, route: RouteRef) -> PyResult<RouteRef> {
        let mut graph = lock_graph(&self.state)?;
        let route = graph.register_port(route.inner);
        Ok(RouteRef { inner: route })
    }

    fn compile_no_lineage_materializer_drop_profile(
        &self,
        route: RouteRef,
        target_route: RouteRef,
    ) -> PyResult<NoLineageMaterializerDropProfile> {
        let graph = lock_graph(&self.state)?;
        if !graph.materializer_pair_is_registered(&route.inner, &target_route.inner) {
            return Err(PyRuntimeError::new_err(
                "materializer profile route pair is not registered",
            ));
        }
        Ok(NoLineageMaterializerDropProfile {
            source_route: route.inner,
            target_route: target_route.inner,
            materialize_generation: graph.materialize_generation,
        })
    }

    fn release_no_lineage_materializer_drop_profile(
        &self,
        _profile: NoLineageMaterializerDropProfile,
    ) {
    }

    #[pyo3(signature = (profile, payload))]
    fn emit_no_lineage_materializer_drop_profile_python(
        &self,
        profile: PyRef<'_, NoLineageMaterializerDropProfile>,
        payload: Vec<u8>,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        if profile.materialize_generation == graph.materialize_generation {
            return Ok(graph.write_single_if_unrouted_known_materializer_drop(
                &profile.source_route,
                &profile.target_route,
                payload,
                ProducerRefCore {
                    producer_id: "python".to_string(),
                    kind: ProducerKind::Application,
                },
                None,
            ));
        }
        Ok(graph.write_single_if_unrouted_and_materializer_drop(
            &profile.source_route,
            &profile.target_route,
            payload,
            ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            },
            None,
        ))
    }

    #[pyo3(signature = (
        route,
        latest_replay_policy,
        durability_class,
        replay_window,
        payload_retention_policy,
        history_limit=None
    ))]
    fn configure_retention(
        &self,
        route: RouteRef,
        latest_replay_policy: String,
        durability_class: String,
        replay_window: String,
        payload_retention_policy: String,
        history_limit: Option<usize>,
    ) -> PyResult<()> {
        validate_nonblank_text("latest_replay_policy", &latest_replay_policy)?;
        validate_nonblank_text("durability_class", &durability_class)?;
        validate_nonblank_text("replay_window", &replay_window)?;
        validate_nonblank_text("payload_retention_policy", &payload_retention_policy)?;
        let mut graph = lock_graph(&self.state)?;
        graph.configure_retention(
            &route.inner,
            RetentionPolicyCore {
                latest_replay_policy,
                durability_class,
                replay_window,
                payload_retention_policy,
                history_limit,
            },
        );
        Ok(())
    }

    fn read(&self, route: RouteRef) -> PyResult<ReadablePort> {
        let mut graph = lock_graph(&self.state)?;
        let route = graph.register_port(route.inner);
        Ok(ReadablePort {
            graph: Arc::clone(&self.state),
            route,
        })
    }

    fn writable_port(&self, route: RouteRef) -> PyResult<WritablePort> {
        let mut graph = lock_graph(&self.state)?;
        let route = graph.register_port(route.inner);
        Ok(WritablePort {
            graph: Arc::clone(&self.state),
            route,
        })
    }

    #[pyo3(signature = (route, payload, producer=None, control_epoch=None))]
    fn emit(
        &self,
        route: RouteRef,
        payload: Vec<u8>,
        producer: Option<ProducerRef>,
        #[pyo3(from_py_with = extract_optional_control_epoch)] control_epoch: Option<u64>,
    ) -> PyResult<Vec<ClosedEnvelope>> {
        let mut graph = lock_graph(&self.state)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        Ok(graph
            .write(&route.inner, payload, producer, control_epoch)
            .into_iter()
            .map(|inner| ClosedEnvelope { inner })
            .collect())
    }

    #[pyo3(signature = (route, payload, producer=None, control_epoch=None))]
    fn emit_single_if_unrouted(
        &self,
        route: RouteRef,
        payload: Vec<u8>,
        producer: Option<ProducerRef>,
        #[pyo3(from_py_with = extract_optional_control_epoch)] control_epoch: Option<u64>,
    ) -> PyResult<Option<ClosedEnvelope>> {
        let mut graph = lock_graph(&self.state)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        Ok(graph
            .write_single_if_unrouted(&route.inner, payload, producer, control_epoch)
            .map(|inner| ClosedEnvelope { inner }))
    }

    #[pyo3(signature = (route, payload, producer=None, control_epoch=None))]
    fn emit_single_if_unrouted_drop(
        &self,
        route: RouteRef,
        payload: Vec<u8>,
        producer: Option<ProducerRef>,
        #[pyo3(from_py_with = extract_optional_control_epoch)] control_epoch: Option<u64>,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        Ok(graph.write_single_if_unrouted_drop(&route.inner, payload, producer, control_epoch))
    }

    #[pyo3(signature = (route, target_route, payload, producer=None, control_epoch=None))]
    fn emit_single_if_unrouted_and_materializer_drop(
        &self,
        route: RouteRef,
        target_route: RouteRef,
        payload: Vec<u8>,
        producer: Option<ProducerRef>,
        #[pyo3(from_py_with = extract_optional_control_epoch)] control_epoch: Option<u64>,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        Ok(graph.write_single_if_unrouted_and_materializer_drop(
            &route.inner,
            &target_route.inner,
            payload,
            producer,
            control_epoch,
        ))
    }

    #[pyo3(signature = (route, target_route, payload))]
    fn emit_single_if_unrouted_and_materializer_drop_python(
        &self,
        route: RouteRef,
        target_route: RouteRef,
        payload: Vec<u8>,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        Ok(graph.write_single_if_unrouted_and_materializer_drop(
            &route.inner,
            &target_route.inner,
            payload,
            ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            },
            None,
        ))
    }

    #[pyo3(signature = (
        route,
        payload,
        producer=None,
        control_epoch=None,
        trace_id=None,
        causality_id=None,
        correlation_id=None,
    ))]
    fn emit_single_if_unrouted_with_lineage_no_parents(
        &self,
        route: RouteRef,
        payload: Vec<u8>,
        producer: Option<ProducerRef>,
        #[pyo3(from_py_with = extract_optional_control_epoch)] control_epoch: Option<u64>,
        trace_id: Option<String>,
        causality_id: Option<String>,
        correlation_id: Option<String>,
    ) -> PyResult<Option<ClosedEnvelope>> {
        let mut graph = lock_graph(&self.state)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        Ok(graph
            .write_single_if_unrouted_with_lineage_no_parents(
                &route.inner,
                payload,
                producer,
                control_epoch,
                trace_id,
                causality_id,
                correlation_id,
            )
            .map(|inner| ClosedEnvelope { inner }))
    }

    #[pyo3(signature = (
        route,
        payload,
        producer=None,
        control_epoch=None,
        trace_id=None,
        causality_id=None,
        correlation_id=None,
    ))]
    fn emit_single_if_unrouted_with_lineage_no_parents_and_materializers(
        &self,
        route: RouteRef,
        payload: Vec<u8>,
        producer: Option<ProducerRef>,
        #[pyo3(from_py_with = extract_optional_control_epoch)] control_epoch: Option<u64>,
        trace_id: Option<String>,
        causality_id: Option<String>,
        correlation_id: Option<String>,
    ) -> PyResult<Option<Vec<ClosedEnvelope>>> {
        let mut graph = lock_graph(&self.state)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        Ok(graph
            .write_single_if_unrouted_with_lineage_no_parents_and_materializers(
                &route.inner,
                payload,
                producer,
                control_epoch,
                trace_id,
                causality_id,
                correlation_id,
            )
            .map(|envelopes| {
                envelopes
                    .into_iter()
                    .map(|inner| ClosedEnvelope { inner })
                    .collect()
            }))
    }

    #[pyo3(signature = (
        route,
        payload,
        producer=None,
        control_epoch=None,
        trace_id=None,
        causality_id=None,
        correlation_id=None,
    ))]
    fn emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop(
        &self,
        route: RouteRef,
        payload: Vec<u8>,
        producer: Option<ProducerRef>,
        #[pyo3(from_py_with = extract_optional_control_epoch)] control_epoch: Option<u64>,
        trace_id: Option<String>,
        causality_id: Option<String>,
        correlation_id: Option<String>,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        Ok(
            graph.write_single_if_unrouted_with_lineage_no_parents_and_materializers_drop(
                &route.inner,
                payload,
                producer,
                control_epoch,
                trace_id,
                causality_id,
                correlation_id,
            ),
        )
    }

    #[pyo3(signature = (
        route,
        payload,
        trace_id=None,
        causality_id=None,
        correlation_id=None,
    ))]
    fn emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop_python(
        &self,
        route: RouteRef,
        payload: Vec<u8>,
        trace_id: Option<String>,
        causality_id: Option<String>,
        correlation_id: Option<String>,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        Ok(
            graph.write_single_if_unrouted_with_lineage_no_parents_and_materializers_drop(
                &route.inner,
                payload,
                ProducerRefCore {
                    producer_id: "python".to_string(),
                    kind: ProducerKind::Application,
                },
                None,
                trace_id,
                causality_id,
                correlation_id,
            ),
        )
    }

    #[pyo3(signature = (
        route,
        payload,
        trace_id,
        causality_id,
        correlation_id=None,
    ))]
    fn emit_single_if_unrouted_with_lineage_ids_no_parents_and_materializers_drop_python(
        &self,
        route: RouteRef,
        payload: Vec<u8>,
        trace_id: String,
        causality_id: String,
        correlation_id: Option<String>,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        Ok(
            graph.write_single_if_unrouted_with_lineage_ids_no_parents_and_materializers_drop(
                &route.inner,
                payload,
                ProducerRefCore {
                    producer_id: "python".to_string(),
                    kind: ProducerKind::Application,
                },
                None,
                trace_id,
                causality_id,
                correlation_id,
            ),
        )
    }

    #[pyo3(signature = (
        route,
        target_route,
        payload,
        trace_id,
        causality_id,
        correlation_id=None,
    ))]
    fn emit_single_if_unrouted_with_lineage_ids_no_parents_and_materializer_drop_python(
        &self,
        route: RouteRef,
        target_route: RouteRef,
        payload: Vec<u8>,
        trace_id: String,
        causality_id: String,
        correlation_id: Option<String>,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        Ok(
            graph.write_single_if_unrouted_with_lineage_ids_no_parents_and_materializer_drop(
                &route.inner,
                &target_route.inner,
                payload,
                ProducerRefCore {
                    producer_id: "python".to_string(),
                    kind: ProducerKind::Application,
                },
                None,
                trace_id,
                causality_id,
                correlation_id,
            ),
        )
    }

    #[pyo3(signature = (source_route, source_seq_source, target_route, producer=None))]
    fn materialize_bytes_one_parent(
        &self,
        source_route: RouteRef,
        source_seq_source: u64,
        target_route: RouteRef,
        producer: Option<ProducerRef>,
    ) -> PyResult<Option<ClosedEnvelope>> {
        let mut graph = lock_graph(&self.state)?;
        let producer = producer
            .map(|producer| producer.inner)
            .unwrap_or(ProducerRefCore {
                producer_id: "python".to_string(),
                kind: ProducerKind::Application,
            });
        Ok(graph
            .materialize_bytes_one_parent(
                &source_route.inner,
                source_seq_source,
                &target_route.inner,
                producer,
            )
            .map(|inner| ClosedEnvelope { inner }))
    }

    fn register_materialize_bytes(
        &self,
        source_route: RouteRef,
        target_route: RouteRef,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        Ok(graph.register_materialize_bytes(&source_route.inner, &target_route.inner))
    }

    fn unregister_materialize_bytes(
        &self,
        source_route: RouteRef,
        target_route: RouteRef,
    ) -> PyResult<bool> {
        let mut graph = lock_graph(&self.state)?;
        Ok(graph.unregister_materialize_bytes(&source_route.inner, &target_route.inner))
    }

    fn register_binding(&self, name: String, binding: WriteBinding) -> PyResult<WriteBinding> {
        let mut graph = lock_graph(&self.state)?;
        graph.register_binding(name, binding.inner.clone());
        Ok(binding)
    }

    #[pyo3(signature = (name, descriptor=None))]
    fn mailbox(&self, name: String, descriptor: Option<MailboxDescriptor>) -> PyResult<Mailbox> {
        validate_nonblank_text("mailbox name", &name)?;
        let descriptor = descriptor.unwrap_or(MailboxDescriptor {
            inner: MailboxDescriptorCore {
                delivery_mode: DeliveryMode::MpscSerial,
                ordering_policy: OrderingPolicy::Fifo,
                overflow_policy: OverflowPolicy::Block,
                capacity: 128,
            },
        });
        let ingress = RouteRefCore {
            namespace: NamespaceRefCore {
                plane: Plane::Write,
                layer: Layer::Internal,
                owner: name.clone(),
            },
            family: "mailbox".to_string(),
            stream: name.clone(),
            variant: Variant::Request,
            schema: SchemaRefCore {
                schema_id: "MailboxIngress".to_string(),
                version: 1,
            },
        };
        let egress = RouteRefCore {
            namespace: NamespaceRefCore {
                plane: Plane::Read,
                layer: Layer::Internal,
                owner: name.clone(),
            },
            family: "mailbox".to_string(),
            stream: name.clone(),
            variant: Variant::Meta,
            schema: SchemaRefCore {
                schema_id: "MailboxEgress".to_string(),
                version: 1,
            },
        };
        let mailbox = MailboxCore {
            ingress,
            egress,
            descriptor: descriptor.inner,
            queue: Default::default(),
            blocked_writes: 0,
            dropped_messages: 0,
            coalesced_messages: 0,
            delivered_messages: 0,
            largest_queue_depth: 0,
        };
        let mut graph = lock_graph(&self.state)?;
        if graph.mailboxes.contains_key(&name) {
            return Err(PyValueError::new_err(format!(
                "mailbox {name:?} already exists"
            )));
        }
        graph.register_mailbox(name.clone(), mailbox);
        Ok(Mailbox {
            graph: Arc::clone(&self.state),
            name,
        })
    }

    fn connect(&self, source: &Bound<'_, PyAny>, sink: &Bound<'_, PyAny>) -> PyResult<bool> {
        let source_route = if let Ok(route) = source.extract::<RouteRef>() {
            route.inner
        } else if let Ok(mailbox) = source.extract::<Mailbox>() {
            let graph = lock_graph(&self.state)?;
            graph
                .mailboxes
                .get(&mailbox.name)
                .map(|mailbox| mailbox.egress.clone())
                .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?
        } else {
            return Err(PyTypeError::new_err("source must be a RouteRef or Mailbox"));
        };
        let sink_route = if let Ok(route) = sink.extract::<RouteRef>() {
            route.inner
        } else if let Ok(mailbox) = sink.extract::<Mailbox>() {
            let graph = lock_graph(&self.state)?;
            graph
                .mailboxes
                .get(&mailbox.name)
                .map(|mailbox| mailbox.ingress.clone())
                .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?
        } else {
            return Err(PyTypeError::new_err("sink must be a RouteRef or Mailbox"));
        };
        let mut graph = lock_graph(&self.state)?;
        Ok(graph.connect(&source_route, &sink_route))
    }

    fn disconnect(&self, source: &Bound<'_, PyAny>, sink: &Bound<'_, PyAny>) -> PyResult<bool> {
        let source_route = if let Ok(route) = source.extract::<RouteRef>() {
            route.inner
        } else if let Ok(mailbox) = source.extract::<Mailbox>() {
            let graph = lock_graph(&self.state)?;
            graph
                .mailboxes
                .get(&mailbox.name)
                .map(|mailbox| mailbox.egress.clone())
                .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?
        } else {
            return Err(PyTypeError::new_err("source must be a RouteRef or Mailbox"));
        };
        let sink_route = if let Ok(route) = sink.extract::<RouteRef>() {
            route.inner
        } else if let Ok(mailbox) = sink.extract::<Mailbox>() {
            let graph = lock_graph(&self.state)?;
            graph
                .mailboxes
                .get(&mailbox.name)
                .map(|mailbox| mailbox.ingress.clone())
                .ok_or_else(|| PyKeyError::new_err("unknown mailbox"))?
        } else {
            return Err(PyTypeError::new_err("sink must be a RouteRef or Mailbox"));
        };
        let mut graph = lock_graph(&self.state)?;
        Ok(graph.disconnect(&source_route, &sink_route))
    }

    fn install(&self, loop_ref: ControlLoop) -> PyResult<()> {
        let mut graph = lock_graph(&self.state)?;
        let name = loop_ref.inner.name.clone();
        if graph.loops.contains_key(&name) {
            return Err(PyValueError::new_err(format!(
                "control loop {name:?} is already installed"
            )));
        }
        graph.loops.insert(name, loop_ref.inner.clone());
        Ok(())
    }

    fn tick_control_loop(&self, name: String) -> PyResult<ClosedEnvelope> {
        let mut graph = lock_graph(&self.state)?;
        let loop_ref = graph
            .loops
            .get_mut(&name)
            .ok_or_else(|| PyKeyError::new_err("unknown control loop"))?;
        let emitted = loop_ref.tick();
        graph.latest.insert(emitted.route.clone(), emitted.clone());
        Ok(ClosedEnvelope { inner: emitted })
    }

    fn catalog(&self) -> PyResult<Vec<RouteRef>> {
        let graph = lock_graph(&self.state)?;
        let QueryResultCore::Catalog(routes) = graph.query(QueryKindCore::Catalog) else {
            return Err(PyRuntimeError::new_err("unexpected catalog response"));
        };
        Ok(routes.into_iter().map(|inner| RouteRef { inner }).collect())
    }

    fn describe_route(&self, route: RouteRef) -> PyResult<PortDescriptor> {
        let graph = lock_graph(&self.state)?;
        let QueryResultCore::DescribeRoute(inner) =
            graph.query(QueryKindCore::DescribeRoute(route.inner))
        else {
            return Err(PyRuntimeError::new_err(
                "unexpected describe_route response",
            ));
        };
        Ok(PortDescriptor { inner })
    }

    fn latest(&self, route: RouteRef) -> PyResult<Option<ClosedEnvelope>> {
        let graph = lock_graph(&self.state)?;
        let QueryResultCore::Latest(inner) = graph.query(QueryKindCore::Latest(route.inner)) else {
            return Err(PyRuntimeError::new_err("unexpected latest response"));
        };
        Ok(inner.map(|inner| ClosedEnvelope { inner }))
    }

    fn replay(&self, route: RouteRef) -> PyResult<Vec<ClosedEnvelope>> {
        let graph = lock_graph(&self.state)?;
        let QueryResultCore::Replay(items) = graph.query(QueryKindCore::Replay(route.inner)) else {
            return Err(PyRuntimeError::new_err("unexpected replay response"));
        };
        Ok(items
            .into_iter()
            .map(|inner| ClosedEnvelope { inner })
            .collect())
    }

    fn retained_payload_count(&self, route: RouteRef) -> PyResult<usize> {
        let graph = lock_graph(&self.state)?;
        Ok(graph.retained_payload_count(&route.inner))
    }

    fn payload_by_id(&self, payload_id: String) -> PyResult<Option<Vec<u8>>> {
        validate_nonblank_text("payload_id", &payload_id)?;
        let graph = lock_graph(&self.state)?;
        Ok(graph.payload_by_id(&payload_id))
    }

    #[pyo3(signature = (route=None))]
    fn retention_snapshot(&self, route: Option<RouteRef>) -> PyResult<Vec<RetentionSnapshot>> {
        let graph = lock_graph(&self.state)?;
        let routes = match route {
            Some(route) => vec![route.inner],
            None => {
                let mut routes = graph.descriptors.keys().cloned().collect::<Vec<_>>();
                routes.sort_by_cached_key(RouteRefCore::display);
                routes
            }
        };
        Ok(routes
            .into_iter()
            .map(|route| RetentionSnapshot {
                inner: graph.retention_snapshot(&route),
            })
            .collect())
    }

    fn retention_violations(&self) -> PyResult<Vec<String>> {
        let graph = lock_graph(&self.state)?;
        Ok(graph.retention_violations())
    }

    fn lineage_intern_value_count(&self) -> PyResult<usize> {
        let graph = lock_graph(&self.state)?;
        Ok(graph.active_lineage_value_count())
    }

    fn topology(&self) -> PyResult<Vec<(String, String)>> {
        let graph = lock_graph(&self.state)?;
        let QueryResultCore::Topology(edges) = graph.query(QueryKindCore::Topology) else {
            return Err(PyRuntimeError::new_err("unexpected topology response"));
        };
        Ok(edges)
    }

    fn validate_graph(&self) -> PyResult<Vec<String>> {
        let graph = lock_graph(&self.state)?;
        let QueryResultCore::ValidateGraph(issues) = graph.query(QueryKindCore::ValidateGraph)
        else {
            return Err(PyRuntimeError::new_err(
                "unexpected validate_graph response",
            ));
        };
        Ok(issues)
    }

    fn credit_snapshot(&self) -> PyResult<Vec<CreditSnapshot>> {
        let graph = lock_graph(&self.state)?;
        Ok(graph
            .credit_snapshot()
            .into_iter()
            .map(|inner| CreditSnapshot { inner })
            .collect())
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct ArchitecturePad {
    name: String,
    topic: String,
    direction: String,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ArchitecturePad {
    #[new]
    #[pyo3(signature = (name, topic, direction="internal".to_string()))]
    fn new(name: String, topic: String, direction: String) -> PyResult<Self> {
        validate_nonblank_text("pad name", &name)?;
        validate_nonblank_text("pad topic", &topic)?;
        if !matches!(direction.as_str(), "input" | "output" | "internal") {
            return Err(PyValueError::new_err(
                "pad direction must be 'input', 'output', or 'internal'",
            ));
        }
        Ok(Self {
            name,
            topic,
            direction,
        })
    }

    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn topic(&self) -> String {
        self.topic.clone()
    }

    #[getter]
    fn direction(&self) -> String {
        self.direction.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Pad(name={:?}, topic={:?}, direction={:?})",
            self.name, self.topic, self.direction
        )
    }

    fn __richcmp__(&self, other: PyRef<'_, ArchitecturePad>, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.name == other.name
                && self.topic == other.topic
                && self.direction == other.direction),
            CompareOp::Ne => Ok(self.name != other.name
                || self.topic != other.topic
                || self.direction != other.direction),
            _ => Err(PyTypeError::new_err(
                "pads only support equality comparison",
            )),
        }
    }
}

macro_rules! native_named_link {
    ($name:ident, $repr:literal, [$($field:ident),+]) => {
        #[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
        #[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
        #[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
        #[derive(Clone)]
        pub struct $name {
            $( $field: String, )+
        }

        #[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
        #[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
        #[pymethods]
        impl $name {
            #[new]
            fn new($( $field: String ),+) -> PyResult<Self> {
                $( validate_nonblank_text(stringify!($field), &$field)?; )+
                Ok(Self { $( $field, )+ })
            }

            $(
                #[getter]
                fn $field(&self) -> String {
                    self.$field.clone()
                }
            )+

            fn __repr__(&self) -> String {
                format!($repr, $( &self.$field ),+)
            }
        }
    };
}

native_named_link!(
    ArchitectureRelay,
    "Relay(name={:?}, source={:?}, target={:?})",
    [name, source, target]
);
native_named_link!(
    ArchitectureVia,
    "Via(name={:?}, source={:?}, target={:?}, boundary={:?})",
    [name, source, target, boundary]
);
native_named_link!(
    ArchitectureGround,
    "Ground(name={:?}, reason={:?})",
    [name, reason]
);
native_named_link!(
    ArchitectureProbe,
    "Probe(name={:?}, target={:?})",
    [name, target]
);

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct ArchitectureRegulator {
    name: String,
    policy: String,
    limit: Option<u64>,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ArchitectureRegulator {
    #[new]
    #[pyo3(signature = (name, policy, limit=None))]
    fn new(name: String, policy: String, limit: Option<u64>) -> PyResult<Self> {
        validate_nonblank_text("regulator name", &name)?;
        validate_nonblank_text("regulator policy", &policy)?;
        if limit == Some(0) {
            return Err(PyValueError::new_err("regulator limit must be positive"));
        }
        Ok(Self {
            name,
            policy,
            limit,
        })
    }

    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn policy(&self) -> String {
        self.policy.clone()
    }

    #[getter]
    fn limit(&self) -> Option<u64> {
        self.limit
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct ArchitectureCapacitor {
    name: String,
    source: String,
    target: String,
    capacity: u64,
    location: Option<String>,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ArchitectureCapacitor {
    #[new]
    #[pyo3(signature = (name, source, target, capacity=1, location=None))]
    fn new(
        name: String,
        source: String,
        target: String,
        capacity: u64,
        location: Option<String>,
    ) -> PyResult<Self> {
        validate_nonblank_text("capacitor name", &name)?;
        validate_nonblank_text("capacitor source", &source)?;
        validate_nonblank_text("capacitor target", &target)?;
        if capacity == 0 {
            return Err(PyValueError::new_err("capacitor capacity must be positive"));
        }
        if let Some(value) = location.as_deref() {
            validate_nonblank_text("capacitor location", value)?;
        }
        Ok(Self {
            name,
            source,
            target,
            capacity,
            location,
        })
    }

    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn source(&self) -> String {
        self.source.clone()
    }

    #[getter]
    fn target(&self) -> String {
        self.target.clone()
    }

    #[getter]
    fn capacity(&self) -> u64 {
        self.capacity
    }

    #[getter]
    fn location(&self) -> Option<String> {
        self.location.clone()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct ArchitectureResistor {
    name: String,
    boundary: String,
    policy: String,
    limit: Option<u64>,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ArchitectureResistor {
    #[new]
    #[pyo3(signature = (name, boundary, policy, limit=None))]
    fn new(name: String, boundary: String, policy: String, limit: Option<u64>) -> PyResult<Self> {
        validate_nonblank_text("resistor name", &name)?;
        validate_nonblank_text("resistor boundary", &boundary)?;
        validate_nonblank_text("resistor policy", &policy)?;
        if limit == Some(0) {
            return Err(PyValueError::new_err("resistor limit must be positive"));
        }
        Ok(Self {
            name,
            boundary,
            policy,
            limit,
        })
    }

    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn boundary(&self) -> String {
        self.boundary.clone()
    }

    #[getter]
    fn policy(&self) -> String {
        self.policy.clone()
    }

    #[getter]
    fn limit(&self) -> Option<u64> {
        self.limit
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct ClockCalibrationSample {
    observed_ns: u64,
    reference_ns: u64,
    temperature_c: Option<f64>,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl ClockCalibrationSample {
    #[new]
    #[pyo3(signature = (observed_ns, reference_ns, temperature_c=None))]
    fn new(observed_ns: u64, reference_ns: u64, temperature_c: Option<f64>) -> Self {
        Self {
            observed_ns,
            reference_ns,
            temperature_c,
        }
    }

    #[getter]
    fn observed_ns(&self) -> u64 {
        self.observed_ns
    }

    #[getter]
    fn reference_ns(&self) -> u64 {
        self.reference_ns
    }

    #[getter]
    fn temperature_c(&self) -> Option<f64> {
        self.temperature_c
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen)]
pub struct SystemTimeProvider;

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl SystemTimeProvider {
    #[new]
    fn new() -> Self {
        Self
    }

    fn now_ns(&self) -> PyResult<u64> {
        current_system_time_ns()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen)]
pub struct NtpTimeProvider {
    server: String,
    port: u16,
    timeout_ms: u64,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl NtpTimeProvider {
    #[new]
    #[pyo3(signature = (server, *, port=123, timeout_ms=1000))]
    fn new(server: String, port: u16, timeout_ms: u64) -> PyResult<Self> {
        validate_nonblank_text("ntp server", &server)?;
        if port == 0 {
            return Err(PyValueError::new_err("ntp port must be positive"));
        }
        if timeout_ms == 0 {
            return Err(PyValueError::new_err("ntp timeout_ms must be positive"));
        }
        Ok(Self {
            server,
            port,
            timeout_ms,
        })
    }

    #[getter]
    fn server(&self) -> String {
        self.server.clone()
    }

    #[getter]
    fn port(&self) -> u16 {
        self.port
    }

    #[getter]
    fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    fn now_ns(&self) -> PyResult<u64> {
        let socket = UdpSocket::bind("0.0.0.0:0").map_err(|error| {
            PyRuntimeError::new_err(format!("failed to bind NTP socket: {error}"))
        })?;
        let timeout = Some(Duration::from_millis(self.timeout_ms));
        socket.set_read_timeout(timeout).map_err(|error| {
            PyRuntimeError::new_err(format!("failed to configure NTP socket: {error}"))
        })?;
        socket.set_write_timeout(timeout).map_err(|error| {
            PyRuntimeError::new_err(format!("failed to configure NTP socket: {error}"))
        })?;

        let mut request = [0_u8; 48];
        request[0] = 0x1b;
        socket
            .send_to(&request, (self.server.as_str(), self.port))
            .map_err(|error| {
                PyRuntimeError::new_err(format!("failed to send NTP request: {error}"))
            })?;

        let mut response = [0_u8; 48];
        let (received, _) = socket.recv_from(&mut response).map_err(|error| {
            PyRuntimeError::new_err(format!("failed to receive NTP response: {error}"))
        })?;
        if received < response.len() {
            return Err(PyRuntimeError::new_err(
                "NTP response was shorter than 48 bytes",
            ));
        }

        const NTP_UNIX_EPOCH_SECONDS: u64 = 2_208_988_800;
        let seconds =
            u32::from_be_bytes([response[40], response[41], response[42], response[43]]) as u64;
        if seconds < NTP_UNIX_EPOCH_SECONDS {
            return Err(PyRuntimeError::new_err("NTP response predates Unix epoch"));
        }
        let fraction =
            u32::from_be_bytes([response[44], response[45], response[46], response[47]]) as u128;
        let nanos = ((fraction * 1_000_000_000_u128) >> 32) as u64;
        (seconds - NTP_UNIX_EPOCH_SECONDS)
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(nanos))
            .ok_or_else(|| PyRuntimeError::new_err("NTP timestamp overflowed nanoseconds"))
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust")]
pub struct MonotonicLogicalClock {
    value: u64,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl MonotonicLogicalClock {
    #[new]
    fn new() -> Self {
        Self { value: 0 }
    }

    fn tick(&mut self) -> u64 {
        self.value += 1;
        self.value
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust")]
pub struct Clock {
    logical: MonotonicLogicalClock,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl Clock {
    #[new]
    fn new() -> Self {
        Self {
            logical: MonotonicLogicalClock::new(),
        }
    }

    fn tick(&mut self) -> u64 {
        self.logical.tick()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen)]
pub struct CalibratedClock {
    samples: Vec<ClockCalibrationSample>,
    slope: f64,
    intercept: f64,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl CalibratedClock {
    #[new]
    #[pyo3(signature = (samples))]
    fn new(samples: Vec<ClockCalibrationSample>) -> PyResult<Self> {
        if samples.is_empty() {
            return Err(PyValueError::new_err(
                "calibrated clock requires at least one true time observation",
            ));
        }
        let (slope, intercept) = fit_clock_calibration(&samples);
        Ok(Self {
            samples,
            slope,
            intercept,
        })
    }

    #[getter]
    fn samples(&self) -> Vec<ClockCalibrationSample> {
        self.samples.clone()
    }

    fn now_ns(&self) -> PyResult<u64> {
        let observed = current_system_time_ns()? as f64;
        let calibrated = self.slope.mul_add(observed, self.intercept);
        if !calibrated.is_finite() || calibrated < 0.0 {
            return Err(PyRuntimeError::new_err("calibrated time is not finite"));
        }
        Ok(calibrated.round() as u64)
    }
}

fn fit_clock_calibration(samples: &[ClockCalibrationSample]) -> (f64, f64) {
    if samples.len() == 1 {
        let sample = &samples[0];
        return (1.0, sample.reference_ns as f64 - sample.observed_ns as f64);
    }
    let count = samples.len() as f64;
    let sum_observed: f64 = samples.iter().map(|sample| sample.observed_ns as f64).sum();
    let sum_reference: f64 = samples
        .iter()
        .map(|sample| sample.reference_ns as f64)
        .sum();
    let mean_observed = sum_observed / count;
    let mean_reference = sum_reference / count;
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for sample in samples {
        let observed_delta = sample.observed_ns as f64 - mean_observed;
        numerator += observed_delta * (sample.reference_ns as f64 - mean_reference);
        denominator += observed_delta * observed_delta;
    }
    if denominator == 0.0 {
        return (1.0, mean_reference - mean_observed);
    }
    let slope = numerator / denominator;
    let intercept = mean_reference - slope * mean_observed;
    (slope, intercept)
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct DataStreamRecord {
    pad_name: Option<String>,
    topic: String,
    payload: Vec<u8>,
    offset: u64,
    process_sequence: u64,
    event_time: i64,
    key: Option<String>,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct FlatBufferField {
    name: String,
    index: u16,
    field_type: String,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl FlatBufferField {
    #[new]
    #[pyo3(signature = (name, index, field_type))]
    fn new(name: String, index: u16, field_type: String) -> PyResult<Self> {
        validate_sql_identifier("flatbuffer field name", &name)?;
        validate_flatbuffer_field_type(&field_type)?;
        Ok(Self {
            name,
            index,
            field_type,
        })
    }

    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn index(&self) -> u16 {
        self.index
    }

    #[getter]
    fn field_type(&self) -> String {
        self.field_type.clone()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct FlatBufferTable {
    name: String,
    fields: Vec<FlatBufferField>,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl FlatBufferTable {
    #[new]
    #[pyo3(signature = (name, fields))]
    fn new(name: String, fields: Vec<FlatBufferField>) -> PyResult<Self> {
        validate_nonblank_text("flatbuffer table name", &name)?;
        if fields.is_empty() {
            return Err(PyValueError::new_err(
                "flatbuffer table must expose at least one field",
            ));
        }
        Ok(Self { name, fields })
    }

    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn fields(&self) -> Vec<FlatBufferField> {
        self.fields.clone()
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl DataStreamRecord {
    #[getter]
    fn pad_name(&self) -> Option<String> {
        self.pad_name.clone()
    }

    #[getter]
    fn topic(&self) -> String {
        self.topic.clone()
    }

    #[getter]
    fn payload(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, &self.payload).into()
    }

    #[getter]
    fn offset(&self) -> u64 {
        self.offset
    }

    #[getter]
    fn process_sequence(&self) -> u64 {
        self.process_sequence
    }

    #[getter]
    fn event_time(&self) -> i64 {
        self.event_time
    }

    #[getter]
    fn key(&self) -> Option<String> {
        self.key.clone()
    }

    #[getter]
    fn seq_source(&self) -> u64 {
        self.offset + 1
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", unsendable)]
pub struct DataStreamProcessor {
    connection: Connection,
    process_sequence: u64,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl DataStreamProcessor {
    #[new]
    fn new() -> PyResult<Self> {
        Self::new_processor()
    }

    #[pyo3(signature = (message, *, event_time=None, key=None))]
    fn ingest(
        &mut self,
        message: &PubSubMessage,
        event_time: Option<i64>,
        key: Option<String>,
    ) -> PyResult<DataStreamRecord> {
        self.ingest_core(&message.inner, event_time, key)
    }

    #[pyo3(signature = (messages))]
    fn ingest_many(&mut self, messages: Vec<PubSubMessage>) -> PyResult<Vec<DataStreamRecord>> {
        messages
            .iter()
            .map(|message| self.ingest_core(&message.inner, None, None))
            .collect()
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: String,
        parameters: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyList>> {
        query_connection(py, &self.connection, &sql, parameters.as_ref())
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn query_one(
        &self,
        py: Python<'_>,
        sql: String,
        parameters: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let rows = query_connection(py, &self.connection, &sql, parameters.as_ref())?;
        let rows = rows.bind(py);
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.get_item(0)?.extract()?)),
            _ => Err(PyValueError::new_err(
                "stream processor SQL returned more than one row",
            )),
        }
    }

    fn latest(&self, topic: String) -> PyResult<Option<DataStreamRecord>> {
        self.latest_core(&topic)
    }
}

impl DataStreamProcessor {
    fn new_processor() -> PyResult<Self> {
        let connection = Connection::open_in_memory().map_err(|error| {
            PyRuntimeError::new_err(format!("failed to open stream SQL store: {error}"))
        })?;
        connection
            .execute_batch(
                r#"
                CREATE TABLE stream_messages (
                    pad_name TEXT,
                    topic TEXT NOT NULL,
                    offset INTEGER NOT NULL,
                    process_sequence INTEGER NOT NULL,
                    event_time INTEGER NOT NULL,
                    message_key TEXT,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (topic, offset)
                );
                "#,
            )
            .map_err(stream_sql_error)?;
        Ok(Self {
            connection,
            process_sequence: 0,
        })
    }

    fn ingest_core(
        &mut self,
        message: &PubSubMessageCore,
        event_time: Option<i64>,
        key: Option<String>,
    ) -> PyResult<DataStreamRecord> {
        self.process_sequence += 1;
        let process_sequence = self.process_sequence;
        let resolved_event_time = event_time.unwrap_or(process_sequence as i64);
        self.connection
            .execute(
                r#"
                INSERT OR REPLACE INTO stream_messages (
                    pad_name,
                    topic,
                    offset,
                    process_sequence,
                    event_time,
                    message_key,
                    payload
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    None::<String>,
                    message.topic,
                    message.offset,
                    process_sequence,
                    resolved_event_time,
                    key,
                    message.payload,
                ],
            )
            .map_err(stream_sql_error)?;
        Ok(DataStreamRecord {
            pad_name: None,
            topic: message.topic.clone(),
            payload: message.payload.clone(),
            offset: message.offset,
            process_sequence,
            event_time: resolved_event_time,
            key,
        })
    }

    fn ingest_board_message(
        &mut self,
        message: &PubSubMessageCore,
        metadata: (Option<i64>, Option<String>, Option<String>),
        flatbuffer_table: Option<&FlatBufferTable>,
    ) -> PyResult<DataStreamRecord> {
        let (event_time, key, pad_name) = metadata;
        self.process_sequence += 1;
        let process_sequence = self.process_sequence;
        let resolved_event_time = event_time.unwrap_or(process_sequence as i64);
        self.connection
            .execute(
                r#"
                INSERT OR REPLACE INTO stream_messages (
                    pad_name,
                    topic,
                    offset,
                    process_sequence,
                    event_time,
                    message_key,
                    payload
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    pad_name,
                    message.topic,
                    message.offset,
                    process_sequence,
                    resolved_event_time,
                    key,
                    message.payload,
                ],
            )
            .map_err(stream_sql_error)?;
        if let Some(table) = flatbuffer_table {
            self.project_flatbuffer_fields(table, message)?;
        }
        Ok(DataStreamRecord {
            pad_name,
            topic: message.topic.clone(),
            payload: message.payload.clone(),
            offset: message.offset,
            process_sequence,
            event_time: resolved_event_time,
            key,
        })
    }

    fn latest_core(&self, topic: &str) -> PyResult<Option<DataStreamRecord>> {
        let mut statement = self
            .connection
            .prepare(
                r#"
                SELECT pad_name, topic, payload, offset, process_sequence, event_time, message_key
                FROM stream_messages
                WHERE topic = ?1
                ORDER BY event_time DESC, process_sequence DESC
                LIMIT 1
                "#,
            )
            .map_err(stream_sql_error)?;
        let mut rows = statement.query(params![topic]).map_err(stream_sql_error)?;
        let Some(row) = rows.next().map_err(stream_sql_error)? else {
            return Ok(None);
        };
        Ok(Some(DataStreamRecord {
            pad_name: row.get(0).map_err(stream_sql_error)?,
            topic: row.get(1).map_err(stream_sql_error)?,
            payload: row.get(2).map_err(stream_sql_error)?,
            offset: row.get(3).map_err(stream_sql_error)?,
            process_sequence: row.get(4).map_err(stream_sql_error)?,
            event_time: row.get(5).map_err(stream_sql_error)?,
            key: row.get(6).map_err(stream_sql_error)?,
        }))
    }

    fn ensure_flatbuffer_columns(&self, table: &FlatBufferTable) -> PyResult<()> {
        for field in &table.fields {
            self.connection
                .execute(
                    &format!(
                        "ALTER TABLE stream_messages ADD COLUMN {} {}",
                        field.name,
                        flatbuffer_sql_type(&field.field_type)
                    ),
                    (),
                )
                .or_else(|error| {
                    if is_duplicate_column_error(&error) {
                        Ok(0)
                    } else {
                        Err(error)
                    }
                })
                .map_err(stream_sql_error)?;
        }
        Ok(())
    }

    fn project_flatbuffer_fields(
        &self,
        table: &FlatBufferTable,
        message: &PubSubMessageCore,
    ) -> PyResult<()> {
        for field in &table.fields {
            let value = decode_flatbuffer_field(&message.payload, field)?;
            self.connection
                .execute(
                    &format!(
                        "UPDATE stream_messages SET {} = ?1 WHERE topic = ?2 AND offset = ?3",
                        field.name
                    ),
                    params![value, message.topic, message.offset],
                )
                .map_err(stream_sql_error)?;
        }
        Ok(())
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", unsendable)]
pub struct PubSubRuntime {
    pubsub: InMemoryPubSubCore,
    processor: DataStreamProcessor,
    metadata_by_message: BTreeMap<StreamMessageKey, StreamMessageMetadata>,
    flatbuffer_tables_by_pad: BTreeMap<String, FlatBufferTable>,
    subscription_name: String,
}

type StreamMessageKey = (String, u64);
type StreamMessageMetadata = (Option<i64>, Option<String>, Option<String>);

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl PubSubRuntime {
    #[new]
    #[pyo3(signature = (*, name="pubsub".to_string(), retained_messages=1024))]
    fn new(name: String, retained_messages: usize) -> PyResult<Self> {
        validate_nonblank_text("pubsub name", &name)?;
        let mut pubsub =
            InMemoryPubSubCore::new(retained_messages).map_err(PyValueError::new_err)?;
        let subscription_name = format!("{name}.stream_processor");
        pubsub
            .subscribe("*".to_string(), Some(subscription_name.clone()), false)
            .map_err(PyValueError::new_err)?;
        Ok(Self {
            pubsub,
            processor: DataStreamProcessor::new_processor()?,
            metadata_by_message: BTreeMap::new(),
            flatbuffer_tables_by_pad: BTreeMap::new(),
            subscription_name,
        })
    }

    fn register_flatbuffer_pad(
        &mut self,
        pad_name: String,
        table: FlatBufferTable,
    ) -> PyResult<()> {
        validate_nonblank_text("pad name", &pad_name)?;
        self.processor.ensure_flatbuffer_columns(&table)?;
        self.flatbuffer_tables_by_pad.insert(pad_name, table);
        Ok(())
    }

    #[pyo3(signature = (topic, payload, *, pad_name=None, event_time=None, key=None))]
    fn publish(
        &mut self,
        topic: String,
        payload: Vec<u8>,
        pad_name: Option<String>,
        event_time: Option<i64>,
        key: Option<String>,
    ) -> PyResult<PubSubDelivery> {
        let delivery = self
            .pubsub
            .publish(topic, payload)
            .map_err(PyValueError::new_err)?;
        self.metadata_by_message.insert(
            (delivery.topic.clone(), delivery.offset),
            (event_time, key, pad_name),
        );
        Ok(PubSubDelivery { inner: delivery })
    }

    fn drain(&mut self) -> PyResult<Vec<DataStreamRecord>> {
        let messages = self
            .pubsub
            .poll(&self.subscription_name, None)
            .map_err(PyValueError::new_err)?;
        let mut records = Vec::with_capacity(messages.len());
        for message in messages {
            let metadata = self
                .metadata_by_message
                .remove(&(message.topic.clone(), message.offset))
                .unwrap_or((None, None, None));
            let flatbuffer_table = metadata
                .2
                .as_ref()
                .and_then(|pad_name| self.flatbuffer_tables_by_pad.get(pad_name));
            records.push(self.processor.ingest_board_message(
                &message,
                metadata,
                flatbuffer_table,
            )?);
        }
        Ok(records)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn query(
        &mut self,
        py: Python<'_>,
        sql: String,
        parameters: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyList>> {
        self.drain()?;
        query_connection(py, &self.processor.connection, &sql, parameters.as_ref())
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn query_one(
        &mut self,
        py: Python<'_>,
        sql: String,
        parameters: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Option<Py<PyDict>>> {
        self.drain()?;
        let rows = query_connection(py, &self.processor.connection, &sql, parameters.as_ref())?;
        let rows = rows.bind(py);
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.get_item(0)?.extract()?)),
            _ => Err(PyValueError::new_err(
                "board SQL returned more than one row",
            )),
        }
    }

    fn latest(&mut self, topic: String) -> PyResult<Option<DataStreamRecord>> {
        self.drain()?;
        self.processor.latest_core(&topic)
    }
}

fn query_connection(
    py: Python<'_>,
    connection: &Connection,
    sql: &str,
    parameters: Option<&Bound<'_, PyDict>>,
) -> PyResult<Py<PyList>> {
    let statement = sql.trim();
    if statement.is_empty() {
        return Err(PyValueError::new_err(
            "stream processor SQL must be non-empty",
        ));
    }
    let operation = statement
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !matches!(operation.as_str(), "select" | "with") {
        return Err(PyValueError::new_err(
            "stream processor SQL must be a SELECT or WITH query",
        ));
    }
    let mut prepared = connection.prepare(statement).map_err(stream_sql_error)?;
    let column_names: Vec<String> = prepared
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let sql_parameters = sql_parameters_from_python(parameters)?;
    let named_parameters: Vec<(&str, &dyn ToSql)> = sql_parameters
        .iter()
        .map(|(name, value)| (name.as_str(), value as &dyn ToSql))
        .collect();
    let mut rows = prepared
        .query(named_parameters.as_slice())
        .map_err(stream_sql_error)?;
    let output = PyList::empty(py);
    while let Some(row) = rows.next().map_err(stream_sql_error)? {
        let item = PyDict::new(py);
        for (index, name) in column_names.iter().enumerate() {
            match row.get_ref(index).map_err(stream_sql_error)? {
                ValueRef::Null => item.set_item(name, py.None())?,
                ValueRef::Integer(value) => item.set_item(name, value)?,
                ValueRef::Real(value) => item.set_item(name, value)?,
                ValueRef::Text(value) => {
                    item.set_item(name, String::from_utf8_lossy(value).as_ref())?
                }
                ValueRef::Blob(value) => item.set_item(name, PyBytes::new(py, value))?,
            }
        }
        output.append(item)?;
    }
    Ok(output.into())
}

fn sql_parameters_from_python(
    parameters: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<(String, rusqlite::types::Value)>> {
    let Some(parameters) = parameters else {
        return Ok(Vec::new());
    };
    let mut values = Vec::with_capacity(parameters.len());
    for (key, value) in parameters.iter() {
        let key = key.extract::<String>()?;
        validate_nonblank_text("SQL parameter name", &key)?;
        let name = if key.starts_with(':') {
            key
        } else {
            format!(":{key}")
        };
        values.push((name, py_to_sql_value(&value)?));
    }
    Ok(values)
}

fn py_to_sql_value(value: &Bound<'_, PyAny>) -> PyResult<rusqlite::types::Value> {
    if value.is_none() {
        return Ok(rusqlite::types::Value::Null);
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(rusqlite::types::Value::Integer(i64::from(
            value.extract::<bool>()?,
        )));
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(rusqlite::types::Value::Integer(value));
    }
    if let Ok(value) = value.extract::<f64>() {
        return Ok(rusqlite::types::Value::Real(value));
    }
    if let Ok(value) = value.extract::<Vec<u8>>() {
        return Ok(rusqlite::types::Value::Blob(value));
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(rusqlite::types::Value::Text(value));
    }
    Err(PyTypeError::new_err(
        "SQL parameters must be None, bool, int, float, str, or bytes",
    ))
}

fn validate_sql_identifier(field: &str, value: &str) -> PyResult<()> {
    validate_nonblank_text(field, value)?;
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(PyValueError::new_err(format!(
            "{field} must be a SQL identifier"
        )));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(PyValueError::new_err(format!(
            "{field} must be a SQL identifier"
        )));
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(PyValueError::new_err(format!(
            "{field} must be a SQL identifier"
        )));
    }
    Ok(())
}

fn validate_flatbuffer_field_type(value: &str) -> PyResult<()> {
    if matches!(
        value,
        "bool" | "float32" | "float64" | "int32" | "int64" | "uint32" | "uint64" | "string"
    ) {
        return Ok(());
    }
    Err(PyValueError::new_err(
        "flatbuffer field_type must be bool, float32, float64, int32, int64, uint32, uint64, or string",
    ))
}

fn flatbuffer_sql_type(field_type: &str) -> &'static str {
    match field_type {
        "float32" | "float64" => "REAL",
        "string" => "TEXT",
        _ => "INTEGER",
    }
}

fn is_duplicate_column_error(error: &rusqlite::Error) -> bool {
    matches!(error, rusqlite::Error::SqliteFailure(_, Some(message)) if message.contains("duplicate column name"))
}

fn decode_flatbuffer_field(
    payload: &[u8],
    field: &FlatBufferField,
) -> PyResult<rusqlite::types::Value> {
    let table_position = read_u32(payload, 0)? as usize;
    ensure_flatbuffer_range(payload, table_position, 4)?;
    let vtable_relative = read_i32(payload, table_position)?;
    if vtable_relative < 0 {
        return Err(PyValueError::new_err(
            "invalid FlatBuffer table vtable offset",
        ));
    }
    let vtable_position = table_position
        .checked_sub(vtable_relative as usize)
        .ok_or_else(|| PyValueError::new_err("invalid FlatBuffer vtable position"))?;
    let vtable_length = read_u16(payload, vtable_position)? as usize;
    let field_vtable_position = 4 + usize::from(field.index) * 2;
    if field_vtable_position + 2 > vtable_length {
        return Ok(rusqlite::types::Value::Null);
    }
    let field_offset = read_u16(payload, vtable_position + field_vtable_position)? as usize;
    if field_offset == 0 {
        return Ok(rusqlite::types::Value::Null);
    }
    let value_position = table_position
        .checked_add(field_offset)
        .ok_or_else(|| PyValueError::new_err("invalid FlatBuffer field position"))?;
    match field.field_type.as_str() {
        "bool" => Ok(rusqlite::types::Value::Integer(i64::from(
            read_u8(payload, value_position)? != 0,
        ))),
        "float32" => Ok(rusqlite::types::Value::Real(f64::from(read_f32(
            payload,
            value_position,
        )?))),
        "float64" => Ok(rusqlite::types::Value::Real(read_f64(
            payload,
            value_position,
        )?)),
        "int32" => Ok(rusqlite::types::Value::Integer(i64::from(read_i32(
            payload,
            value_position,
        )?))),
        "int64" => Ok(rusqlite::types::Value::Integer(read_i64(
            payload,
            value_position,
        )?)),
        "uint32" => Ok(rusqlite::types::Value::Integer(i64::from(read_u32(
            payload,
            value_position,
        )?))),
        "uint64" => {
            let value = read_u64(payload, value_position)?;
            let value = i64::try_from(value).map_err(|_| {
                PyValueError::new_err("uint64 FlatBuffer value exceeds SQL integer range")
            })?;
            Ok(rusqlite::types::Value::Integer(value))
        }
        "string" => Ok(rusqlite::types::Value::Text(read_flatbuffer_string(
            payload,
            value_position,
        )?)),
        _ => Err(PyValueError::new_err("unsupported FlatBuffer field type")),
    }
}

fn read_flatbuffer_string(payload: &[u8], value_position: usize) -> PyResult<String> {
    let relative = read_u32(payload, value_position)? as usize;
    let string_position = value_position
        .checked_add(relative)
        .ok_or_else(|| PyValueError::new_err("invalid FlatBuffer string position"))?;
    let length = read_u32(payload, string_position)? as usize;
    let data_position = string_position + 4;
    ensure_flatbuffer_range(payload, data_position, length)?;
    std::str::from_utf8(&payload[data_position..data_position + length])
        .map(str::to_owned)
        .map_err(|error| PyValueError::new_err(format!("invalid FlatBuffer UTF-8 string: {error}")))
}

fn ensure_flatbuffer_range(payload: &[u8], start: usize, length: usize) -> PyResult<()> {
    let end = start
        .checked_add(length)
        .ok_or_else(|| PyValueError::new_err("FlatBuffer offset overflowed"))?;
    if end > payload.len() {
        return Err(PyValueError::new_err("FlatBuffer field is out of bounds"));
    }
    Ok(())
}

fn read_u8(payload: &[u8], start: usize) -> PyResult<u8> {
    ensure_flatbuffer_range(payload, start, 1)?;
    Ok(payload[start])
}

fn read_u16(payload: &[u8], start: usize) -> PyResult<u16> {
    ensure_flatbuffer_range(payload, start, 2)?;
    Ok(u16::from_le_bytes([payload[start], payload[start + 1]]))
}

fn read_u32(payload: &[u8], start: usize) -> PyResult<u32> {
    ensure_flatbuffer_range(payload, start, 4)?;
    Ok(u32::from_le_bytes([
        payload[start],
        payload[start + 1],
        payload[start + 2],
        payload[start + 3],
    ]))
}

fn read_u64(payload: &[u8], start: usize) -> PyResult<u64> {
    ensure_flatbuffer_range(payload, start, 8)?;
    Ok(u64::from_le_bytes([
        payload[start],
        payload[start + 1],
        payload[start + 2],
        payload[start + 3],
        payload[start + 4],
        payload[start + 5],
        payload[start + 6],
        payload[start + 7],
    ]))
}

fn read_i32(payload: &[u8], start: usize) -> PyResult<i32> {
    Ok(read_u32(payload, start)? as i32)
}

fn read_i64(payload: &[u8], start: usize) -> PyResult<i64> {
    Ok(read_u64(payload, start)? as i64)
}

fn read_f32(payload: &[u8], start: usize) -> PyResult<f32> {
    Ok(f32::from_bits(read_u32(payload, start)?))
}

fn read_f64(payload: &[u8], start: usize) -> PyResult<f64> {
    Ok(f64::from_bits(read_u64(payload, start)?))
}

fn stream_sql_error(error: rusqlite::Error) -> PyErr {
    PyRuntimeError::new_err(format!("stream SQL operation failed: {error}"))
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct PubSubMessage {
    inner: PubSubMessageCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl PubSubMessage {
    #[new]
    #[pyo3(signature = (topic, payload, offset=0))]
    fn new(topic: String, payload: Vec<u8>, offset: u64) -> PyResult<Self> {
        validate_nonblank_text("pubsub topic", &topic)?;
        if payload.is_empty() {
            return Err(PyValueError::new_err(
                "pubsub payload must be non-empty bytes",
            ));
        }
        Ok(Self {
            inner: PubSubMessageCore {
                topic,
                payload,
                offset,
            },
        })
    }

    #[getter]
    fn topic(&self) -> String {
        self.inner.topic.clone()
    }

    #[getter]
    fn payload(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, &self.inner.payload).into()
    }

    #[getter]
    fn offset(&self) -> u64 {
        self.inner.offset
    }

    fn __repr__(&self) -> String {
        format!(
            "PubSubMessage(topic={:?}, offset={}, payload_len={})",
            self.inner.topic,
            self.inner.offset,
            self.inner.payload.len()
        )
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct PubSubSubscription {
    inner: PubSubSubscriptionCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl PubSubSubscription {
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[getter]
    fn topic(&self) -> String {
        self.inner.topic.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "PubSubSubscription(name={:?}, topic={:?})",
            self.inner.name, self.inner.topic
        )
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust", frozen, from_py_object)]
#[derive(Clone)]
pub struct PubSubDelivery {
    inner: PubSubDeliveryCore,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl PubSubDelivery {
    #[getter]
    fn topic(&self) -> String {
        self.inner.topic.clone()
    }

    #[getter]
    fn offset(&self) -> u64 {
        self.inner.offset
    }

    #[getter]
    fn delivered_to(&self) -> Vec<String> {
        self.inner.delivered_to.clone()
    }

    #[getter]
    fn subscriber_count(&self) -> usize {
        self.inner.delivered_to.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "PubSubDelivery(topic={:?}, offset={}, subscriber_count={})",
            self.inner.topic,
            self.inner.offset,
            self.inner.delivered_to.len()
        )
    }
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyclass)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyclass(module = "manyfold._manyfold_rust")]
pub struct InMemoryPubSub {
    inner: Arc<Mutex<InMemoryPubSubCore>>,
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pymethods)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pymethods]
impl InMemoryPubSub {
    #[new]
    #[pyo3(signature = (*, retained_messages=1024))]
    fn new(retained_messages: usize) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                InMemoryPubSubCore::new(retained_messages).map_err(PyValueError::new_err)?,
            )),
        })
    }

    #[getter]
    fn retained_messages(&self) -> PyResult<usize> {
        Ok(lock_pubsub(&self.inner)?.retained_messages())
    }

    #[getter]
    fn message_count(&self) -> PyResult<usize> {
        Ok(lock_pubsub(&self.inner)?.message_count())
    }

    #[getter]
    fn subscriber_count(&self) -> PyResult<usize> {
        Ok(lock_pubsub(&self.inner)?.subscriber_count())
    }

    #[pyo3(signature = (topic, *, name=None, replay_from_beginning=false))]
    fn subscribe(
        &self,
        topic: String,
        name: Option<String>,
        replay_from_beginning: bool,
    ) -> PyResult<PubSubSubscription> {
        let mut pubsub = lock_pubsub(&self.inner)?;
        Ok(PubSubSubscription {
            inner: pubsub
                .subscribe(topic, name, replay_from_beginning)
                .map_err(PyValueError::new_err)?,
        })
    }

    fn unsubscribe(&self, subscription: String) -> PyResult<bool> {
        let mut pubsub = lock_pubsub(&self.inner)?;
        pubsub
            .unsubscribe(&subscription)
            .map_err(PyValueError::new_err)
    }

    fn publish(&self, topic: String, payload: Vec<u8>) -> PyResult<PubSubDelivery> {
        let mut pubsub = lock_pubsub(&self.inner)?;
        Ok(PubSubDelivery {
            inner: pubsub
                .publish(topic, payload)
                .map_err(PyValueError::new_err)?,
        })
    }

    #[pyo3(signature = (subscription, *, max_messages=None))]
    fn poll(
        &self,
        subscription: String,
        max_messages: Option<usize>,
    ) -> PyResult<Vec<PubSubMessage>> {
        let mut pubsub = lock_pubsub(&self.inner)?;
        Ok(pubsub
            .poll(&subscription, max_messages)
            .map_err(PyValueError::new_err)?
            .into_iter()
            .map(|inner| PubSubMessage { inner })
            .collect())
    }

    #[pyo3(signature = (topic=None))]
    fn latest(&self, topic: Option<String>) -> PyResult<Option<PubSubMessage>> {
        let pubsub = lock_pubsub(&self.inner)?;
        Ok(pubsub
            .latest(topic.as_deref())
            .map_err(PyValueError::new_err)?
            .map(|inner| PubSubMessage { inner }))
    }

    fn topic_offsets(&self) -> PyResult<BTreeMap<String, u64>> {
        Ok(lock_pubsub(&self.inner)?.topic_offsets())
    }
}

fn lock_pubsub(
    state: &Arc<Mutex<InMemoryPubSubCore>>,
) -> PyResult<std::sync::MutexGuard<'_, InMemoryPubSubCore>> {
    state
        .lock()
        .map_err(|_| PyRuntimeError::new_err("pubsub mutex poisoned"))
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyfunction)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyfunction]
fn bridge_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyfunction)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyfunction]
fn parse_sql_statement(py: Python<'_>, sql: &str) -> PyResult<Py<PyDict>> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|error| PyValueError::new_err(format!("invalid SQL: {error}")))?;
    if statements.len() != 1 {
        return Err(PyValueError::new_err(
            "SQL planner requires exactly one statement",
        ));
    }
    let statement = &statements[0];
    let kind = match statement {
        Statement::Query(_) => "select",
        Statement::Insert(_) => "insert",
        Statement::Update { .. } => "update",
        Statement::Delete(_) => "delete",
        _ => "unsupported",
    };
    let result = PyDict::new(py);
    result.set_item("kind", kind)?;
    result.set_item("sql", statement.to_string())?;
    Ok(result.into())
}

#[pymodule]
fn _manyfold_rust(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(bridge_version, module)?)?;
    module.add_function(wrap_pyfunction!(parse_sql_statement, module)?)?;
    module.add_class::<PyPlane>()?;
    module.add_class::<PyLayer>()?;
    module.add_class::<PyVariant>()?;
    module.add_class::<PyProducerKind>()?;
    module.add_class::<PyTaintDomain>()?;
    module.add_class::<NamespaceRef>()?;
    module.add_class::<SchemaRef>()?;
    module.add_class::<RouteRef>()?;
    module.add_class::<ProducerRef>()?;
    module.add_class::<RuntimeRef>()?;
    module.add_class::<ClockDomainRef>()?;
    module.add_class::<PayloadRef>()?;
    module.add_class::<TaintMark>()?;
    module.add_class::<ScheduleGuard>()?;
    module.add_class::<ClosedEnvelope>()?;
    module.add_class::<OpenedEnvelope>()?;
    module.add_class::<PortDescriptor>()?;
    module.add_class::<ReadablePort>()?;
    module.add_class::<WritablePort>()?;
    module.add_class::<WriteBinding>()?;
    module.add_class::<MailboxDescriptor>()?;
    module.add_class::<CreditSnapshot>()?;
    module.add_class::<RetentionSnapshot>()?;
    module.add_class::<NoLineageMaterializerDropProfile>()?;
    module.add_class::<ArchitecturePad>()?;
    module.add_class::<ArchitectureRelay>()?;
    module.add_class::<ArchitectureVia>()?;
    module.add_class::<ArchitectureRegulator>()?;
    module.add_class::<ArchitectureGround>()?;
    module.add_class::<ArchitectureProbe>()?;
    module.add_class::<ArchitectureCapacitor>()?;
    module.add_class::<ArchitectureResistor>()?;
    module.add_class::<ClockCalibrationSample>()?;
    module.add_class::<SystemTimeProvider>()?;
    module.add_class::<NtpTimeProvider>()?;
    module.add_class::<MonotonicLogicalClock>()?;
    module.add_class::<Clock>()?;
    module.add_class::<CalibratedClock>()?;
    module.add_class::<FlatBufferField>()?;
    module.add_class::<FlatBufferTable>()?;
    module.add_class::<DataStreamRecord>()?;
    module.add_class::<DataStreamProcessor>()?;
    module.add_class::<PubSubRuntime>()?;
    module.add_class::<PubSubMessage>()?;
    module.add_class::<PubSubSubscription>()?;
    module.add_class::<PubSubDelivery>()?;
    module.add_class::<InMemoryPubSub>()?;
    module.add_class::<Mailbox>()?;
    module.add_class::<ControlLoop>()?;
    module.add_class::<Graph>()?;
    Ok(())
}

#[cfg(feature = "stub-gen")]
define_stub_info_gatherer!(stub_info);
