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
  maxPeers = 8,
} = {}) {
  const identity = new NodeIdentity("test-cluster", "browser-a", "instance-a");
  const config = new ClientConfig(identity, placement, staticPeers, 32, maxPeers);
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

test("passes a purpose-bound host credential into enrollment", async () => {
  const host = new HostCapabilities("browser");
  const peer = new PeerEndpoint("credentialed.example", 7443);
  host.setEnrollmentCredentialIssuer((request) => {
    assert.equal(request.purpose, "manyfold.peer-enrollment.v1");
    assert.equal(request.identity.nodeId, "browser-a");
    assert.equal(request.candidate.host, "credentialed.example");
    assert.equal("bytes" in request, false);
    return {
      purpose: request.purpose,
      token: "opaque-short-lived-credential",
      expiresAtUnixMs: Date.now() + 60_000,
    };
  });
  host.setEnrollment((request) => {
    assert.equal(request.credential.purpose, "manyfold.peer-enrollment.v1");
    assert.equal(request.credential.token, "opaque-short-lived-credential");
    return {
      authenticated: true,
      identity: {
        clusterId: "test-cluster",
        nodeId: "credentialed-peer",
        instanceId: "credentialed-peer-instance",
      },
    };
  });
  const client = makeClient({ host, staticPeers: [peer] });

  const status = await client.start();

  assert.equal(host.hasEnrollmentCredentialIssuer, true);
  assert.equal(status.state, "ready");
  assert.equal(status.authenticatedPeerCount, 1);
  assert.equal("machineSignerSocket" in host, false);
  assert.equal("sign" in host, false);
  await client.shutdown();
});

test("reports an unavailable host signer as degraded", async () => {
  const host = new HostCapabilities("electron");
  let enrollmentCalls = 0;
  host.setEnrollmentCredentialIssuer(() => {
    throw new Error("machine signer socket is unavailable");
  });
  host.setEnrollment(() => {
    enrollmentCalls += 1;
    throw new Error("must not enroll without a credential");
  });
  const client = makeClient({
    host,
    staticPeers: [new PeerEndpoint("desktop-peer.example", 7443)],
  });

  const status = await client.start();

  assert.equal(status.state, "degraded");
  assert.equal(status.authenticatedPeerCount, 0);
  assert.equal(status.failureCount, 1);
  assert.match(status.failures[0], /enrollment signer unavailable/);
  assert.match(status.failures[0], /machine signer socket is unavailable/);
  assert.equal(enrollmentCalls, 0);
  await client.shutdown();
});

test("rejects credentials from another signing purpose without logging them", async () => {
  const host = new HostCapabilities("desktop");
  let enrollmentCalls = 0;
  host.setEnrollmentCredentialIssuer(() => ({
    purpose: "generic-signing.v1",
    token: "secret-that-must-not-reach-diagnostics",
    expiresAtUnixMs: Date.now() + 60_000,
  }));
  host.setEnrollment(() => {
    enrollmentCalls += 1;
    throw new Error("must not enroll with another signing purpose");
  });
  const client = makeClient({
    host,
    staticPeers: [new PeerEndpoint("wrong-purpose.example", 7443)],
  });

  const status = await client.start();

  assert.equal(status.state, "degraded");
  assert.match(status.failures[0], /credential purpose/);
  assert.doesNotMatch(status.failures[0], /secret-that-must-not/);
  assert.equal(enrollmentCalls, 0);
  await client.shutdown();
});

test("bounds expired-credential diagnostics and skips enrollment", async () => {
  const host = new HostCapabilities("browser");
  const peers = Array.from(
    { length: 140 },
    (_, index) => new PeerEndpoint(`expired-${index}.example`, 7443),
  );
  let enrollmentCalls = 0;
  host.setDiscovery(() => peers);
  host.setEnrollmentCredentialIssuer((request) => ({
    purpose: request.purpose,
    token: "expired-credential",
    expiresAtUnixMs: Date.now() - 1,
  }));
  host.setEnrollment(() => {
    enrollmentCalls += 1;
    throw new Error("must not enroll with an expired credential");
  });
  const client = makeClient({ host, maxPeers: peers.length });

  const status = await client.start();

  assert.equal(status.state, "degraded");
  assert.equal(status.discoveredPeerCount, peers.length);
  assert.equal(status.failureCount, 128);
  assert.equal(status.failures.length, 128);
  assert.match(status.failures[0], /enrollment credential expired/);
  assert.equal(enrollmentCalls, 0);
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
