use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use manyfold::core::{
    GraphCore, Layer, NamespaceRefCore, Plane, ProducerKind, ProducerRefCore, RetentionPolicyCore,
    RouteRefCore, SchemaRefCore, Variant,
};

struct CountingAllocator;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !pointer.is_null() {
            let old_size = layout.size();
            if new_size >= old_size {
                LIVE_BYTES.fetch_add(new_size - old_size, Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub(old_size - new_size, Ordering::Relaxed);
            }
        }
        pointer
    }
}

#[test]
fn sparse_unrouted_write_has_zero_net_allocations_after_warmup() {
    let mut graph = GraphCore::default();
    let route = route();
    graph.configure_retention(
        &route,
        RetentionPolicyCore {
            latest_replay_policy: "bounded_history".to_string(),
            durability_class: "memory".to_string(),
            replay_window: "latest 8".to_string(),
            payload_retention_policy: "retain_replay".to_string(),
            history_limit: Some(8),
        },
    );
    let producer = producer();
    let payload = vec![7_u8; 32];

    assert_eq!(graph.retained_lineage_event_count(), 0);
    for _ in 0..128 {
        assert!(graph.write_single_if_unrouted_drop(
            &route,
            payload.clone(),
            producer.clone(),
            None
        ));
    }

    let before = LIVE_BYTES.load(Ordering::Relaxed);
    run_sparse_write(&mut graph, &route, &producer, &payload);
    let after = LIVE_BYTES.load(Ordering::Relaxed);

    assert_eq!(after, before);
    assert_eq!(graph.retained_lineage_event_count(), 0);
}

fn run_sparse_write(
    graph: &mut GraphCore,
    route: &RouteRefCore,
    producer: &ProducerRefCore,
    payload: &[u8],
) {
    assert!(graph.write_single_if_unrouted_drop(route, payload.to_vec(), producer.clone(), None));
}

fn route() -> RouteRefCore {
    RouteRefCore {
        namespace: NamespaceRefCore {
            plane: Plane::Read,
            layer: Layer::Logical,
            owner: "heart".to_string(),
        },
        family: "runtime".to_string(),
        stream: "zero_net_alloc".to_string(),
        variant: Variant::Event,
        schema: SchemaRefCore {
            schema_id: "ZeroNetAllocBytes".to_string(),
            version: 1,
        },
    }
}

fn producer() -> ProducerRefCore {
    ProducerRefCore {
        producer_id: "heart".to_string(),
        kind: ProducerKind::Application,
    }
}
