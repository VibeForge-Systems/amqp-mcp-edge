import { z } from 'zod';

import { edgeLibraryName, edgeLibraryVersion } from '../version.js';

import { canonicalJsonSha256 } from './canonical-json.js';
import type { BaseEdgeEnvelope } from './envelopes.js';
import { createEnvelopeSchema } from './envelopes.js';
import { distHashSchema, timestampSchema } from './primitives.js';

/** One package release installed on a node. */
export interface InstalledPackage {
  packageId: string;
  packageVersion: string;
  /** The release the control plane installed it from; absent for a package installed another way. */
  releaseId?: string;
  /** The dist hash the node computed over what it has installed. */
  distHash: string;
}

/**
 * What a node can run: the packages it has installed, its runtimes and this library's version.
 * `node.hello` carries it in full, and `node.heartbeat` carries its digest.
 */
export interface NodeCapabilitySnapshot {
  edgeLibrary: {
    name: string;
    version: string;
  };
  /** Runtime name to version, such as `{ node: '22.20.0' }`. At least one entry. */
  runtimes: Record<string, string>;
  /** Sorted by packageId, then packageVersion, when built with `createCapabilitySnapshot`. */
  packages: InstalledPackage[];
}

/** Published by a node after each connection to the broker. */
export interface NodeHelloPayload {
  siteId: string;
  nodeId: string;
  /** Identifies one run of the node process; the same across reconnects, new after a restart. */
  sessionId: string;
  /** Starts at 1 and increases with every lifecycle message of the session. */
  sequence: number;
  /** When the session started. */
  startedAt: string;
  /** How often the node sends `node.heartbeat`. */
  heartbeatIntervalMs: number;
  capabilities: NodeCapabilitySnapshot;
  /** `computeCapabilitiesDigest(capabilities)`. */
  capabilitiesDigest: string;
}

/** Published by a node every `heartbeatIntervalMs` while it is connected. */
export interface NodeHeartbeatPayload {
  siteId: string;
  nodeId: string;
  sessionId: string;
  sequence: number;
  heartbeatIntervalMs: number;
  /** The digest of the node's current snapshot. */
  capabilitiesDigest: string;
  /** The full snapshot. Required when it changed since the node's previous lifecycle message. */
  capabilities?: NodeCapabilitySnapshot;
}

export type NodeHelloEvent = BaseEdgeEnvelope<'node.hello', NodeHelloPayload>;
export type NodeHeartbeatEvent = BaseEdgeEnvelope<'node.heartbeat', NodeHeartbeatPayload>;

/** The digest format: `sha256:` and 64 lowercase hex digits. */
export const capabilitiesDigestPattern = /^sha256:[0-9a-f]{64}$/;

/**
 * The digest of a capability snapshot: `sha256:` followed by the lowercase hex SHA-256 of the
 * snapshot's RFC 8785 (JCS) canonical JSON, over every member the snapshot carries, unknown ones
 * included. Arrays keep their order, so the same packages in another order give another digest.
 */
export function computeCapabilitiesDigest(snapshot: NodeCapabilitySnapshot): string {
  return canonicalJsonSha256(snapshot);
}

function comparePackages(left: InstalledPackage, right: InstalledPackage): number {
  if (left.packageId !== right.packageId) {
    return left.packageId < right.packageId ? -1 : 1;
  }

  if (left.packageVersion !== right.packageVersion) {
    return left.packageVersion < right.packageVersion ? -1 : 1;
  }

  return 0;
}

/**
 * Builds a snapshot for this process: this library's name and version, `node` set to the running
 * Node.js version (plus any `runtimes` given, which win), and the packages sorted by packageId and
 * then packageVersion, so that the same set always gives the same digest.
 */
export function createCapabilitySnapshot(input: {
  packages?: readonly InstalledPackage[];
  runtimes?: Record<string, string>;
} = {}): NodeCapabilitySnapshot {
  return {
    edgeLibrary: { name: edgeLibraryName, version: edgeLibraryVersion },
    runtimes: { node: process.versions.node, ...input.runtimes },
    packages: [...(input.packages ?? [])].map((item) => ({ ...item })).sort(comparePackages),
  };
}

export const installedPackageSchema = z
  .object({
    packageId: z.string().min(1),
    packageVersion: z.string().min(1),
    releaseId: z.string().min(1).optional(),
    distHash: distHashSchema,
  })
  .passthrough();

export const nodeCapabilitySnapshotSchema = z
  .object({
    edgeLibrary: z
      .object({
        name: z.string().min(1),
        version: z.string().min(1),
      })
      .passthrough(),
    runtimes: z
      .record(z.string().min(1), z.string().min(1))
      .refine((runtimes) => Object.keys(runtimes).length > 0, { message: 'must name at least one runtime' }),
    packages: z.array(installedPackageSchema),
  })
  .passthrough();

const capabilitiesDigestSchema = z.string().regex(capabilitiesDigestPattern, {
  message: 'must be sha256: followed by 64 lowercase hex digits',
});

const lifecycleFields = {
  siteId: z.string().min(1),
  nodeId: z.string().min(1),
  sessionId: z.string().min(1),
  sequence: z.number().int().positive(),
  heartbeatIntervalMs: z.number().int().positive(),
  capabilitiesDigest: capabilitiesDigestSchema,
};

// A message that carries the snapshot must carry its digest, so the control plane can compare a
// later heartbeat's digest with the snapshot it stored.
function checkDigest(
  payload: { capabilities?: NodeCapabilitySnapshot; capabilitiesDigest: string },
  context: z.RefinementCtx,
): void {
  if (payload.capabilities === undefined) {
    return;
  }

  const expected = computeCapabilitiesDigest(payload.capabilities);

  if (payload.capabilitiesDigest !== expected) {
    context.addIssue({
      code: 'custom',
      path: ['capabilitiesDigest'],
      message: `does not match capabilities: their digest is ${expected}`,
    });
  }
}

export const nodeHelloPayloadSchema = z
  .object({
    ...lifecycleFields,
    startedAt: timestampSchema,
    capabilities: nodeCapabilitySnapshotSchema,
  })
  .passthrough()
  .superRefine(checkDigest);

export const nodeHeartbeatPayloadSchema = z
  .object({
    ...lifecycleFields,
    capabilities: nodeCapabilitySnapshotSchema.optional(),
  })
  .passthrough()
  .superRefine(checkDigest);

export const nodeHelloEnvelopeSchema = createEnvelopeSchema('node.hello', nodeHelloPayloadSchema);
export const nodeHeartbeatEnvelopeSchema = createEnvelopeSchema('node.heartbeat', nodeHeartbeatPayloadSchema);
