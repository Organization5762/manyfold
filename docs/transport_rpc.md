# Coordinator RPC transport

`manyfold.architecture.transport_rpc` adds a bounded request/response endpoint
over one dedicated `TcpTransport`. It is intended for coordinator operations
such as allocation, membership, leases, and control-plane queries. The endpoint
owns the transport's receive loop, so PubSub traffic must use a different
transport link.

```python
from manyfold.architecture.transport_rpc import RpcEndpoint

server_rpc = RpcEndpoint(server_transport)
client_rpc = RpcEndpoint(client_transport)
if not client_rpc.wait_until_ready(timeout=2.0):
    raise RuntimeError("RPC session handshake did not complete")
server_rpc.register(
    "coordinator",
    "allocate",
    lambda request, cancellation: request.payload.upper(),
)

try:
    worker = client_rpc.call(
        "coordinator",
        "allocate",
        b"worker-7",
        timeout_seconds=2.0,
    )
    print(worker.decode())
finally:
    client_rpc.close()
    server_rpc.close()
```

Sample output:

```text
WORKER-7
```

## Contract

The wire protocol has four typed records:

- `RpcRequest` carries service, method, opaque bytes, a correlation ID, a
  relative execution timeout, and source/target session epochs.
- `RpcResponse` carries successful opaque bytes.
- `RpcErrorRecord` carries a stable error code, readable message, and retryable
  flag. Callers receive it as `RpcRemoteError`.
- `RpcCancel` propagates explicit cancellation, local timeout, endpoint
  disposal, or lost-session cleanup.

Requests use binary length-prefixed metadata rather than JSON or base64. The
transport's protocol/version handshake, node identity, mutual TLS, payload
limit, framing, and ordered delivery still apply. RPC adds a bidirectional
session hello tied to each transport connection. Both peers must acknowledge
the current source and target epochs before `RpcHealth.is_ready` becomes true.

`RpcConfig` independently bounds:

- client in-flight calls;
- registered handlers;
- worker threads;
- queued server requests;
- send waits, receive polling, default deadlines, and shutdown waits.

No executor with an unbounded work queue is used. A request is retained only
after acquiring an in-flight slot, and a server request is retained only while
queued or active. Terminal responses, errors, timeout, cancellation,
disconnect, and disposal all remove the correlation entry and release its
payload.

## Deadlines, cancellation, and reconnects

`RpcEndpoint.call(...)` is the blocking convenience operation.
`start_call(...)` returns `RpcCall`, whose `result()` observes the original
deadline and whose `cancel(...)` propagates a typed cancellation record.
The endpoint receiver also expires abandoned `RpcCall` handles, so callers do
not have to enter `result()` to release their in-flight slot and payload.
Handlers receive `RpcCancellation`; long-running handlers should call
`raise_if_cancelled()`, inspect `is_cancelled`, or wait through
`RpcCancellation.wait(...)`.

Cancellation is cooperative because Python cannot safely terminate arbitrary
handler code. `close()` cancels all queued and active work, releases retained
requests, and joins the receiver and workers. It raises `RpcShutdownTimeout`
when a handler ignores cancellation past the configured shutdown deadline.

Calls belong to the transport session on which they were sent. Disconnect or
reconnect fails older pending calls with `RpcDisconnected`; the endpoint never
silently replays an RPC whose idempotency it does not know. New calls may start
after `TcpTransport` reconnects and the RPC hello completes. Every record names
both its source and intended target session. Queued requests and responses from
an old socket session are therefore discarded even if `TcpTransport` delivers
them after reconnect. A restarted process must use a new
`NodeIdentity.instance_id`, as required by the transport identity contract.

## Remote error semantics

Built-in remote codes are:

| Code | Meaning | Retryable |
| --- | --- | --- |
| `not_found` | No handler is registered for the service and method. | No |
| `overloaded` | The bounded server request queue is full. | Yes |
| `duplicate_request` | The correlation ID is already queued or active. | No |
| `deadline_exceeded` | The request expired before or during execution. | Context-dependent |
| `cancelled` | The handler observed propagated caller cancellation. | No |
| `handler_error` | The handler raised or returned a non-bytes result. | No |
| `response_too_large` | The handler result exceeds the transport payload limit. | No |

Transport/session loss is a local `RpcDisconnected`, local capacity exhaustion
is `RpcOverloaded`, local deadline expiry is `RpcTimeout`, and explicit local
cancellation is `RpcCancelled`.

## Operational boundary

The endpoint provides bounded execution and clear failure semantics, but it
does not claim exactly-once side effects. A request can reach a handler before
the caller observes a disconnect, and cancellation can arrive after the side
effect. Production mutating handlers therefore need an idempotency policy and a
durable correlation/result journal supplied by the delivery layer.

The application also remains responsible for authorization per service/method,
payload schema/version negotiation, domain error mapping, payload redaction,
metrics export, and deployment-specific latency/capacity targets. Unexpected
handler exception details remain local in `RpcHealth.last_error`; the peer sees
only the stable `handler_error` code. `RpcHealth` exposes bounded capacity and
outcome counters for that integration without retaining an event history.
