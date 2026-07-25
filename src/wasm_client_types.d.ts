export type ClientHostKind = "browser" | "electron" | "desktop";
export type ClientLifecycleState =
  | "stopped"
  | "starting"
  | "ready"
  | "degraded"
  | "shutting_down";

export interface DiscoveryRequest {
  readonly identity: {
    readonly clusterId: string;
    readonly nodeId: string;
    readonly instanceId: string;
  };
  readonly staticPeers: ReadonlyArray<{
    readonly host: string;
    readonly port: number;
    readonly serverName?: string;
  }>;
  readonly isCancelled: () => boolean;
}

export interface EnrollmentRequest {
  readonly identity: DiscoveryRequest["identity"];
  readonly candidate: DiscoveryRequest["staticPeers"][number];
  readonly isCancelled: () => boolean;
}

export interface EnrollmentResult {
  readonly authenticated: boolean;
  readonly identity?: DiscoveryRequest["identity"];
  readonly error?: string;
}

export type DiscoverPeers = (
  request: DiscoveryRequest,
) => Promise<ReadonlyArray<PeerEndpoint>> | ReadonlyArray<PeerEndpoint>;
export type EnrollPeer = (
  request: EnrollmentRequest,
) => Promise<EnrollmentResult> | EnrollmentResult;
export type ScheduleThreadCallback = (
  threadName: string,
  callback: () => void,
) => void;
export type SpawnNativeWorker = (request: {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly runtimeId: string;
  readonly retainedMessages: string;
}) => unknown;
export type ShutdownHost = (request: {
  readonly identity: DiscoveryRequest["identity"];
  readonly authenticatedPeerCount: number;
}) => Promise<void> | void;

export interface HostCapabilities {
  setDiscovery(callback: DiscoverPeers): void;
  setEnrollment(callback: EnrollPeer): void;
  setThreadScheduler(callback: ScheduleThreadCallback): void;
  setNativeWorkerSpawner(callback: SpawnNativeWorker): void;
  setShutdown(callback: ShutdownHost): void;
}

export interface ManyfoldClient {
  onStatus(
    callback: (status: ClientStatus) => void,
    placement?: CallbackPlacement | null,
  ): ClientStatusSubscription;
  start(): Promise<ClientStatus>;
  shutdown(): Promise<boolean>;
}
