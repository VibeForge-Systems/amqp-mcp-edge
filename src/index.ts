export type {
  EdgeChannelConfig,
  EventRoutingType,
  ExchangeConfig,
  ParsedEventRoutingKey,
  ProvisionedNode,
  QueueNamingStrategy,
  RoutingKeySelector,
  RoutingKeyStrategy,
  TopologyMode,
} from './config/index.js';
export {
  buildConsumerQueueName,
  buildJobBindingKeys,
  buildJobRoutingKey,
  buildNodeKey,
  buildRoutingKey,
  checkEventIdentity,
  defaultAmqpUsername,
  defaultExchangeConfig,
  defaultQueueNamingStrategy,
  defaultRoutingKeyStrategy,
  eventRoutingTypes,
  parseEventRoutingKey,
  resolveConfig,
  resolveUserId,
  sanitizeRoutingSegment,
  topologyModes,
  validateConfig,
} from './config/index.js';

export { edgeLibraryName, edgeLibraryVersion } from './version.js';

export type { TopologyEntity, TopologyEntityKind } from './topology/index.js';
export { EdgeTopologyError } from './topology/index.js';

export type {
  BaseEdgeEnvelope,
  CompatibilityMetadata,
  CompatibilityValidationOptions,
  CompatibilityValidationResult,
  EdgeEnvelope,
  EdgeFailureCode,
  EdgeMessageType,
  ExecutionAttemptMetadata,
  ExecutionJobEnvelope,
  ExecutionJobPayload,
  InstalledPackage,
  JobAckMessage,
  JobAckPayload,
  JobFailureEvent,
  JobFailurePayload,
  JobProgressEvent,
  JobProgressPayload,
  JobResultEvent,
  JobResultPayload,
  MessageEnvelopeVersion,
  NodeCapabilitySnapshot,
  NodeHeartbeatEvent,
  NodeHeartbeatPayload,
  NodeHelloEvent,
  NodeHelloPayload,
  PackageInstallInput,
  PackageInstallJob,
  PackageInstallResultOutput,
  PackageReleaseIdentity,
  TargetSelector,
  ToolJob,
  ToolResultOutput,
} from './contracts/index.js';
export {
  assertCompatibleEnvelope,
  canonicalJson,
  capabilitiesDigestPattern,
  computeCapabilitiesDigest,
  createCapabilitySnapshot,
  distHashPattern,
  EdgeConventionError,
  edgeFailureCodes,
  edgeMessageTypes,
  EdgeUnsupportedMessageTypeError,
  isEdgeMessageType,
  isPackageInstallJob,
  isToolJob,
  nodeCapabilitySnapshotSchema,
  packageInstallCapability,
  packageInstallInputSchema,
  packageInstallResultOutputSchema,
  parsePackageInstallJob,
  parseToolJob,
  parseToolResultOutput,
  toolNamePattern,
  toolResultOutputSchema,
  validateCompatibility,
  validateEnvelope,
} from './contracts/index.js';

export type { EdgeJobHandler, JobDeliveryContext } from './consumer/index.js';
export { EdgeJobConsumer } from './consumer/index.js';
export { EdgeEventPublisher } from './publisher/index.js';

export type { EdgeChannel } from './connection/index.js';
export { createEdgeChannel, EdgeChannelConnectionManager } from './connection/index.js';

export type { EdgeChannelHealth } from './health/index.js';
export { createHealthSnapshot } from './health/index.js';

export type { MockDeliveryRecord, MockEdgeChannel, MockPublishedEvent } from './testing/index.js';
export { createMockEdgeChannel } from './testing/index.js';