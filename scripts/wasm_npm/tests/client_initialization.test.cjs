const assert = require("node:assert/strict");
const test = require("node:test");

const {
  CallbackPlacement,
  ClientConfig,
  HostCapabilities,
  ManyfoldClient,
  NodeIdentity,
  PeerEndpoint,
  PubSub,
} = require("../nodejs/manyfold.js");

function makeClient({
  host = new HostCapabilities("browser"),
  placement = CallbackPlacement.inline(),
  staticPeers = [],
} = {}) {
  const identity = new NodeIdentity("test-cluster", "browser-a", "instance-a");
  const config = new ClientConfig(identity, placement, staticPeers, 32, 8);
  return new ManyfoldClient(config, host);
}

test("initializes a local client and processes lifecycle callbacks", async () => {
  const client = makeClient();
  const states = [];
  const subscription = client.onStatus((status) => states.push(status.state));

  const status = await client.start();

  assert.equal(status.state, "ready");
  assert.equal(status.authenticatedPeerCount, 0);
  assert.deepEqual(states, ["stopped", "starting", "ready"]);
  assert.throws(() => client.start(), /already started/);

  const events = client.pubsub("client.events");
  events.subscribe("test", false);
  events.publish(new Uint8Array([1, 2, 3]));
  assert.deepEqual(Array.from(events.poll("test")[0].payload), [1, 2, 3]);

  assert.equal(subscription.dispose(), true);
  await client.shutdown();
});

test("uses host discovery results only after host enrollment authenticates them", async () => {
  const host = new HostCapabilities("browser");
  host.setDiscovery((request) => {
    assert.equal(request.identity.clusterId, "test-cluster");
    assert.equal(request.staticPeers.length, 1);
    return [
      new PeerEndpoint("discovered.example", 7443, "discovered.example"),
      new PeerEndpoint("static.example", 7443, "static.example"),
    ];
  });
  const enrolled = [];
  host.setEnrollment((request) => {
    enrolled.push(request.candidate.host);
    return {
      authenticated: true,
      identity: {
        clusterId: "test-cluster",
        nodeId: `peer-${enrolled.length}`,
        instanceId: `peer-instance-${enrolled.length}`,
      },
    };
  });
  const client = makeClient({
    host,
    staticPeers: [new PeerEndpoint("static.example", 7443, "static.example")],
  });

  const status = await client.start();

  assert.equal(status.state, "ready");
  assert.equal(status.discoveredPeerCount, 2);
  assert.equal(status.authenticatedPeerCount, 2);
  assert.deepEqual(enrolled, ["static.example", "discovered.example"]);
  assert.deepEqual(
    Array.from(client.authenticatedPeers()).map((identity) => identity.nodeId),
    ["peer-1", "peer-2"],
  );
  await client.shutdown();
});

test("reports authentication failure as degraded instead of trusting a candidate", async () => {
  const host = new HostCapabilities("browser");
  host.setDiscovery(() => [new PeerEndpoint("untrusted.example", 7443)]);
  host.setEnrollment(() => ({
    authenticated: false,
    error: "certificate identity rejected",
  }));
  const client = makeClient({ host });

  const status = await client.start();

  assert.equal(status.state, "degraded");
  assert.equal(status.discoveredPeerCount, 1);
  assert.equal(status.authenticatedPeerCount, 0);
  assert.equal(status.failureCount, 1);
  assert.match(status.failures[0], /certificate identity rejected/);
  await client.shutdown();
});

test("cancels an in-progress host discovery pass", async () => {
  const host = new HostCapabilities("browser");
  let finishDiscovery;
  let discoveryCalls = 0;
  host.setDiscovery(() => {
    discoveryCalls += 1;
    if (discoveryCalls > 1) {
      return [];
    }
    return new Promise((resolve) => {
      finishDiscovery = resolve;
    });
  });
  const client = makeClient({ host });

  const starting = client.start();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(client.status().state, "starting");
  assert.equal(client.cancelStart(), true);
  finishDiscovery([]);

  const status = await starting;
  assert.equal(status.state, "stopped");
  assert.match(status.detail, /cancelled/);
  assert.equal(client.cancelStart(), false);

  await client.start();
  await client.shutdown();
});

test("uses an injected host scheduler for thread-placed PubSub callbacks", async () => {
  const host = new HostCapabilities("browser");
  const scheduled = [];
  host.setThreadScheduler((name, callback) => {
    scheduled.push(name);
    queueMicrotask(callback);
  });
  const client = makeClient({ host });
  await client.start();
  const events = client.pubsub("scheduled.events");
  const received = [];
  const subscription = events.callback(
    (message) => received.push(Array.from(message.payload)),
    CallbackPlacement.spawnedThread("host-worker"),
    false,
  );

  events.publish(new Uint8Array([7, 8]));
  await Promise.resolve();

  assert.deepEqual(scheduled, ["host-worker"]);
  assert.deepEqual(received, [[7, 8]]);
  subscription.dispose();
  await client.shutdown();
});

test("browser hosts cannot claim native spawning and standalone WASM has no thread", () => {
  const browser = new HostCapabilities("browser");
  assert.throws(
    () => browser.setNativeWorkerSpawner(() => undefined),
    /browser hosts cannot provide native worker spawning/,
  );

  const events = new PubSub("standalone.events", 8);
  events.callback(
    () => undefined,
    CallbackPlacement.spawnedThread("not-a-native-thread"),
    false,
  );
  assert.throws(
    () => events.publish(new Uint8Array([1])),
    /host-provided thread scheduler/,
  );
});

test("desktop hosts inject native worker spawning without WASM creating a process", async () => {
  const desktop = new HostCapabilities("desktop");
  let spawnRequest;
  desktop.setNativeWorkerSpawner((request) => {
    spawnRequest = request;
    return { pid: 42 };
  });
  const client = makeClient({ host: desktop });
  await client.start();

  const runtime = client.pubsub("desktop.runtime");
  const child = runtime.spawnRustWorker("manyfold-worker", ["--listen", "7443"]);

  assert.deepEqual(child, { pid: 42 });
  assert.equal(spawnRequest.command, "manyfold-worker");
  assert.deepEqual(spawnRequest.args, ["--listen", "7443"]);
  assert.equal(spawnRequest.runtimeId, "test-cluster:browser-a");
  await client.shutdown();
});

test("shutdown disposes callbacks, host capabilities, and the local runtime", async () => {
  const host = new HostCapabilities("desktop");
  let shutdownRequest;
  host.setShutdown((request) => {
    shutdownRequest = request;
  });
  const client = makeClient({ host });
  const events = client.pubsub("shutdown.events");
  await client.start();

  assert.equal(await client.shutdown(), true);
  assert.equal(client.isDisposed, true);
  assert.equal(client.status().state, "stopped");
  assert.equal(shutdownRequest.identity.nodeId, "browser-a");
  assert.equal(shutdownRequest.authenticatedPeerCount, 0);
  assert.throws(() => events.publish(new Uint8Array([1])), /shut down/);
  assert.equal(await client.shutdown(), false);
});
