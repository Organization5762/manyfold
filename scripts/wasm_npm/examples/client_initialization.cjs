const {
  CallbackPlacement,
  ClientConfig,
  HostCapabilities,
  ManyfoldClient,
  NodeIdentity,
  PeerEndpoint,
} = require("../nodejs/manyfold.js");

async function run() {
  const host = new HostCapabilities("browser");
  host.setDiscovery(({ staticPeers }) => staticPeers);
  host.setEnrollmentCredentialIssuer(({ purpose }) => ({
    purpose,
    token: "example-host-issued-credential",
    expiresAtUnixMs: Date.now() + 60_000,
  }));
  host.setEnrollment(({ candidate, credential }) => ({
    authenticated: true,
    identity: {
      clusterId: "heart",
      nodeId: `peer-at-${candidate.host}-${credential.purpose}`,
      instanceId: "example-peer-instance",
    },
  }));

  const config = new ClientConfig(
    new NodeIdentity("heart", "browser-client", "example-browser-instance"),
    CallbackPlacement.mainThread(),
    [new PeerEndpoint("desktop-host.example", 7443, "desktop-host.example")],
    128,
    16,
  );
  const client = new ManyfoldClient(config, host);
  const statusSubscription = client.onStatus((status) => {
    console.log(
      `${status.state}: ${status.authenticatedPeerCount} authenticated peer(s)`,
    );
  });

  const status = await client.start();
  if (!status.isReady) {
    throw new Error(status.detail);
  }

  const input = client.pubsub("heart.input");
  input.publish(new Uint8Array([1, 2, 3]));

  statusSubscription.dispose();
  await client.shutdown();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
