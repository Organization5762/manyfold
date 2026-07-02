# @organization5762/manyfold

WebAssembly bindings for Manyfold PubSub APIs.

`PubSub` is the cross-environment data structure API. Internally it sends
endpoint-addressed byte requests to the Manyfold runtime surface, so browser,
Electron, and Python hosts can converge on the same worker protocol without
application code talking directly to the proxy layer.

```js
import {
  CallbackPlacement,
  Clock,
  Lock,
  PubSub,
} from "@organization5762/manyfold";

const input = new PubSub("heart.input");
input.subscribe("browser", false);
input.publish(new Uint8Array([1, 2, 3]));

const messages = input.poll("browser");
console.log(messages[0].payload);
```

The default import uses the bundler build. Host-specific builds are also
published when an application needs different wasm-pack loader glue:

```js
import { PubSub } from "@organization5762/manyfold/web";
import { PubSub as NodePubSub } from "@organization5762/manyfold/nodejs";
```

Callback subscriptions are available for client code that wants Manyfold to
deliver new messages directly. Inline callbacks run during `publish()`,
`mainThread()` callbacks are scheduled through `queueMicrotask`, and
`spawnedThread(name)` uses host async scheduling in the browser build.

```js
const subscription = input.callback(
  (message) => {
    console.log(message.payload);
  },
  CallbackPlacement.mainThread(),
);

input.publish(new Uint8Array([4, 5, 6]));
subscription.dispose();
```

`Lock` and `Clock` are also available directly:

```js
const globalBootLock = new Lock("heart.boot");
const globalClock = new Clock("heart.clock");
```

`PubSub` also exposes its own infrastructure lock and clock. These are single
canonical endpoints owned by the PubSub runtime, not arbitrary child namespaces.
For example, `input.lock()` addresses `heart.input.infrastructure.lock`.

```js
const bootLock = input.lock();
const clock = input.clock();
const lease = bootLock.take("browser", false);
try {
  input.publish(new Uint8Array([1, 2, 3]));
} finally {
  lease.release();
}

console.log(clock.tick());
console.log(clock.nowNs());
console.log(globalClock.tick());
```

Electron and other desktop hosts can let WASM bootstrap a native Rust worker by
installing a privileged host spawner. Browser hosts should leave this unset; the
call then fails with an explicit unsupported error instead of pretending process
creation is portable WASM behavior.

```js
input.setDesktopSpawner(({ command, args, runtimeId, retainedMessages }) => {
  return window.manyfoldDesktop.spawnWorker({
    command,
    args,
    runtimeId,
    retainedMessages,
  });
});

const child = input.spawnRustWorker("/usr/local/bin/manyfold-worker", [
  "--runtime-id",
  "heart",
]);
```

The initial package supports raw PubSub endpoints, callback subscriptions, lock
take/release, and clock tick/time reads. SQL and Variable endpoints are part of
the core worker protocol and will be added as runtime-backed endpoints.
