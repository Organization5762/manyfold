use std::sync::{Arc, Mutex};

use crate::core::{
    ClockDomainRefCore, ClosedEnvelopeCore, ControlLoopCore, CreditSnapshotCore, DeliveryMode,
    GraphCore, Layer, MailboxCore, MailboxDescriptorCore, NamespaceRefCore, OpenedEnvelopeCore,
    OrderingPolicy, OverflowPolicy, Plane, PortDescriptorCore, ProducerKind, ProducerRefCore,
    QueryKindCore, QueryResultCore, RouteRefCore, RuntimeRefCore, ScheduleConditionCore,
    ScheduleGuardCore, SchemaRefCore, TaintDomain, TaintMarkCore, Variant, WriteBindingCore,
};
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;

#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::define_stub_info_gatherer;

fn lock_graph<'a>(
    state: &'a Arc<Mutex<GraphCore>>,
) -> PyResult<std::sync::MutexGuard<'a, GraphCore>> {
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
        Self { inner: Variant::Event }
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
    fn new(plane: PyPlane, layer: PyLayer, owner: String) -> Self {
        Self {
            inner: NamespaceRefCore {
                plane: plane.inner,
                layer: layer.inner,
                owner,
            },
        }
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
    fn new(schema_id: String, version: u32) -> Self {
        Self {
            inner: SchemaRefCore { schema_id, version },
        }
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
    fn new(producer_id: String, kind: PyProducerKind) -> Self {
        Self {
            inner: ProducerRefCore {
                producer_id,
                kind: kind.inner,
            },
        }
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
    fn new(runtime_id: String) -> Self {
        Self {
            inner: RuntimeRefCore { runtime_id },
        }
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
    fn new(clock_domain_id: String) -> Self {
        Self {
            inner: ClockDomainRefCore { clock_domain_id },
        }
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
        logical_length_bytes: u64,
        codec_id: String,
        inline_bytes: Vec<u8>,
    ) -> Self {
        Self {
            inner: crate::core::PayloadRefCore {
                payload_id,
                logical_length_bytes,
                codec_id,
                inline_bytes,
            },
        }
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
    fn new(domain: PyTaintDomain, value_id: String, origin_id: String) -> Self {
        Self {
            inner: TaintMarkCore {
                domain: domain.inner,
                value_id,
                origin_id,
            },
        }
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
    fn not_before_epoch(epoch: u64) -> Self {
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
        control_epoch: Option<u64>,
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
        capacity: usize,
        delivery_mode: String,
        ordering_policy: String,
        overflow_policy: String,
    ) -> PyResult<Self> {
        let delivery_mode = match delivery_mode.as_str() {
            "mpsc_serial" => DeliveryMode::MpscSerial,
            "mpmc_unique" => DeliveryMode::MpmcUnique,
            "mpmc_replicated" => DeliveryMode::MpmcReplicated,
            "key_affine" => DeliveryMode::KeyAffine,
            _ => return Err(PyValueError::new_err("unsupported delivery_mode")),
        };
        let ordering_policy = match ordering_policy.as_str() {
            "fifo" => OrderingPolicy::Fifo,
            "priority_stable" => OrderingPolicy::PriorityStable,
            "weighted_fair" => OrderingPolicy::WeightedFair,
            "round_robin_by_producer" => OrderingPolicy::RoundRobinByProducer,
            "keyed_fifo" => OrderingPolicy::KeyedFifo,
            "latest_only" => OrderingPolicy::LatestOnly,
            "unordered" => OrderingPolicy::Unordered,
            _ => return Err(PyValueError::new_err("unsupported ordering_policy")),
        };
        let overflow_policy = match overflow_policy.as_str() {
            "block" => OverflowPolicy::Block,
            "drop_oldest" => OverflowPolicy::DropOldest,
            "drop_newest" => OverflowPolicy::DropNewest,
            "coalesce_latest" => OverflowPolicy::CoalesceLatest,
            "deadline_drop" => OverflowPolicy::DeadlineDrop,
            "spill_to_store" => OverflowPolicy::SpillToStore,
            "reject_write" => OverflowPolicy::RejectWrite,
            _ => return Err(PyValueError::new_err("unsupported overflow_policy")),
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
    fn new(name: String, read_routes: Vec<RouteRef>, write_request: RouteRef) -> Self {
        Self {
            inner: ControlLoopCore {
                name,
                read_routes: read_routes.into_iter().map(|route| route.inner).collect(),
                write_route: write_request.inner,
                epoch: 0,
            },
        }
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
        control_epoch: Option<u64>,
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

    fn register_binding(&self, name: String, binding: WriteBinding) -> PyResult<WriteBinding> {
        let mut graph = lock_graph(&self.state)?;
        graph.register_binding(name, binding.inner.clone());
        Ok(binding)
    }

    #[pyo3(signature = (name, descriptor=None))]
    fn mailbox(&self, name: String, descriptor: Option<MailboxDescriptor>) -> PyResult<Mailbox> {
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
        };
        let mut graph = lock_graph(&self.state)?;
        graph.register_mailbox(name.clone(), mailbox);
        Ok(Mailbox {
            graph: Arc::clone(&self.state),
            name,
        })
    }

    fn connect(&self, source: &Bound<'_, PyAny>, sink: &Bound<'_, PyAny>) -> PyResult<()> {
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
        graph.connect(&source_route, &sink_route);
        Ok(())
    }

    fn install(&self, loop_ref: ControlLoop) -> PyResult<()> {
        let mut graph = lock_graph(&self.state)?;
        graph
            .loops
            .insert(loop_ref.inner.name.clone(), loop_ref.inner.clone());
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

#[cfg_attr(feature = "stub-gen", pyo3_stub_gen_derive::gen_stub_pyfunction)]
#[cfg_attr(not(feature = "stub-gen"), pyo3_stub_gen_derive::remove_gen_stub)]
#[pyfunction]
fn bridge_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _manyfold_rust(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(bridge_version, module)?)?;
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
    module.add_class::<Mailbox>()?;
    module.add_class::<ControlLoop>()?;
    module.add_class::<Graph>()?;
    Ok(())
}

#[cfg(feature = "stub-gen")]
define_stub_info_gatherer!(stub_info);
