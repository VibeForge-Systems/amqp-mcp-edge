import { ZodError } from 'zod';

import type { CompatibilityMetadata, CompatibilityValidationOptions, CompatibilityValidationResult, ExecutionAttemptMetadata } from './compatibility.js';
import type { BaseEdgeEnvelope, EdgeMessageType, MessageEnvelopeVersion } from './envelopes.js';
import {
  edgeMessageTypes,
} from './envelopes.js';
import type { JobAckMessage, JobAckPayload, JobFailureEvent, JobFailurePayload, JobProgressEvent, JobProgressPayload, JobResultEvent, JobResultPayload } from './events.js';
import {
  jobAckEnvelopeSchema,
  jobFailureEnvelopeSchema,
  jobProgressEnvelopeSchema,
  jobResultEnvelopeSchema,
} from './events.js';
import type { ExecutionJobEnvelope, ExecutionJobPayload, TargetSelector } from './jobs.js';
import { executionJobEnvelopeSchema } from './jobs.js';
import type { NodeHeartbeatEvent, NodeHelloEvent } from './node.js';
import { nodeHeartbeatEnvelopeSchema, nodeHelloEnvelopeSchema } from './node.js';
import { validateCompatibility } from './compatibility.js';

export type {
  CompatibilityMetadata,
  CompatibilityValidationOptions,
  CompatibilityValidationResult,
  ExecutionAttemptMetadata,
} from './compatibility.js';
export { validateCompatibility } from './compatibility.js';
export type { BaseEdgeEnvelope, EdgeMessageType, MessageEnvelopeVersion } from './envelopes.js';
export { edgeMessageTypes } from './envelopes.js';
export type { TargetSelector, ExecutionJobPayload, ExecutionJobEnvelope } from './jobs.js';
export type {
  JobAckPayload,
  JobProgressPayload,
  JobResultPayload,
  JobFailurePayload,
  JobAckMessage,
  JobProgressEvent,
  JobResultEvent,
  JobFailureEvent,
} from './events.js';
export type {
  InstalledPackage,
  NodeCapabilitySnapshot,
  NodeHeartbeatEvent,
  NodeHeartbeatPayload,
  NodeHelloEvent,
  NodeHelloPayload,
} from './node.js';
export {
  capabilitiesDigestPattern,
  computeCapabilitiesDigest,
  createCapabilitySnapshot,
  nodeCapabilitySnapshotSchema,
} from './node.js';
export type { PackageReleaseIdentity } from './primitives.js';
export { distHashPattern } from './primitives.js';
export type {
  EdgeFailureCode,
  PackageInstallInput,
  PackageInstallJob,
  PackageInstallResultOutput,
  ToolJob,
  ToolResultOutput,
} from './conventions.js';
export {
  edgeFailureCodes,
  EdgeConventionError,
  isPackageInstallJob,
  isToolJob,
  packageInstallCapability,
  packageInstallInputSchema,
  packageInstallResultOutputSchema,
  parsePackageInstallJob,
  parseToolJob,
  parseToolResultOutput,
  toolNamePattern,
  toolResultOutputSchema,
} from './conventions.js';
export { canonicalJson } from './canonical-json.js';

export type EdgeEnvelope =
  | ExecutionJobEnvelope
  | JobAckMessage
  | JobProgressEvent
  | JobResultEvent
  | JobFailureEvent
  | NodeHelloEvent
  | NodeHeartbeatEvent;

const envelopeSchemaMap = {
  'execution.job': executionJobEnvelopeSchema,
  'execution.ack': jobAckEnvelopeSchema,
  'execution.progress': jobProgressEnvelopeSchema,
  'execution.result': jobResultEnvelopeSchema,
  'execution.failure': jobFailureEnvelopeSchema,
  'node.hello': nodeHelloEnvelopeSchema,
  'node.heartbeat': nodeHeartbeatEnvelopeSchema,
} as const;

/**
 * The envelope names a `messageType` this version of the library does not know. Under the
 * compatibility rules a consumer ignores such a message (acks it and drops it) rather than treating
 * it as invalid: it may be a message type added after this version.
 */
export class EdgeUnsupportedMessageTypeError extends Error {
  public readonly messageType: unknown;

  public constructor(messageType: unknown) {
    super(`unsupported messageType: ${String(messageType)}`);
    this.name = 'EdgeUnsupportedMessageTypeError';
    this.messageType = messageType;
  }
}

export function isEdgeMessageType(value: unknown): value is EdgeMessageType {
  return typeof value === 'string' && edgeMessageTypes.includes(value as EdgeMessageType);
}

/**
 * Validates an envelope against the schema of its `messageType`. Throws an
 * `EdgeUnsupportedMessageTypeError` for a `messageType` this version does not know, and an `Error`
 * whose message starts `invalid <messageType> envelope:` for one that fails its schema.
 */
export function validateEnvelope(payload: unknown): EdgeEnvelope {
  if (typeof payload !== 'object' || payload === null || !('messageType' in payload)) {
    throw new Error('payload is not a valid edge envelope');
  }

  const messageType = (payload as { messageType?: unknown }).messageType;

  if (!isEdgeMessageType(messageType)) {
    throw new EdgeUnsupportedMessageTypeError(messageType);
  }

  try {
    return envelopeSchemaMap[messageType].parse(payload) as EdgeEnvelope;
  } catch (error) {
    if (error instanceof ZodError) {
      throw new Error(`invalid ${messageType} envelope: ${error.message}`);
    }

    throw error;
  }
}

export function assertCompatibleEnvelope(
  payload: unknown,
  options: CompatibilityValidationOptions = {},
): EdgeEnvelope {
  const envelope = validateEnvelope(payload);
  const result = validateCompatibility(envelope.compatibility, options);

  if (!result.compatible) {
    throw new Error(result.reasons.join('; '));
  }

  return envelope;
}
