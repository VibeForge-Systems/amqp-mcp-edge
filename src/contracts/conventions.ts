import { z } from 'zod';

import type { ExecutionJobEnvelope } from './jobs.js';
import type { PackageReleaseIdentity } from './primitives.js';
import { distHashSchema, packageReleaseIdentitySchema, timestampSchema } from './primitives.js';

/**
 * Standard `code` values for `execution.failure`. A node may use others; these have the meaning
 * docs/PROTOCOL.md gives them, so a control plane can act on them without knowing the node.
 */
export const edgeFailureCodes = {
  /** The job's input does not satisfy the capability's input contract. */
  invalidInput: 'INVALID_INPUT',
  /** The node does not provide the capability, or not the version or package release asked for. */
  unsupportedCapability: 'UNSUPPORTED_CAPABILITY',
  /** The job ran longer than its `timeoutMs` and was stopped. */
  timeout: 'TIMEOUT',
  /** The tool answered with an `input_required` result, which a job cannot satisfy. */
  inputRequired: 'INPUT_REQUIRED',
  /** The handler failed for a reason no other code covers. */
  handlerError: 'HANDLER_ERROR',
  /** The package could not be downloaded. */
  packageDownloadFailed: 'PACKAGE_DOWNLOAD_FAILED',
  /** The signed download URL had expired, or the store refused it. */
  packageUrlExpired: 'PACKAGE_URL_EXPIRED',
  /** The node does not implement the algorithm named by the dist hash's tag. */
  packageHashUnsupported: 'PACKAGE_HASH_UNSUPPORTED',
  /** The dist hash the node computed differs from the expected one; nothing was installed. */
  packageHashMismatch: 'PACKAGE_HASH_MISMATCH',
  /** The package was downloaded and verified, but installing it failed. */
  packageInstallFailed: 'PACKAGE_INSTALL_FAILED',
} as const;

export type EdgeFailureCode = (typeof edgeFailureCodes)[keyof typeof edgeFailureCodes];

/** A job does not follow the convention its capability selects; `issues` lists each problem. */
export class EdgeConventionError extends Error {
  public readonly convention: 'package.install' | 'tool';

  public readonly issues: string[];

  public constructor(convention: 'package.install' | 'tool', issues: string[]) {
    super(`job does not follow the ${convention} convention: ${issues.join('; ')}`);
    this.name = 'EdgeConventionError';
    this.convention = convention;
    this.issues = issues;
  }
}

function describeIssues(error: z.ZodError, prefix: string): string[] {
  return error.issues.map((issue) => {
    const path = [prefix, ...issue.path.map(String)].filter(Boolean).join('.');
    return `${path}: ${issue.message}`;
  });
}

// --- Deploy jobs: capability { kind: 'task', name: 'package.install' } ----------------------------

/** The capability of a deploy job. */
export const packageInstallCapability = { kind: 'task', name: 'package.install' } as const;

/** The `input` of a deploy job. */
export interface PackageInstallInput {
  /** The release to install. */
  release: PackageReleaseIdentity;
  download: {
    /**
     * A signed `https` URL for the release archive. It is a credential: never log it, and never
     * copy it into an event.
     */
    url: string;
    /** When the URL stops working, if the control plane knows. */
    expiresAt?: string;
  };
  /** The dist hash the node must compute over the archive's contents before installing anything. */
  distHash: string;
}

/** The `output` of the `execution.result` that answers a deploy job. */
export interface PackageInstallResultOutput {
  release: PackageReleaseIdentity;
  /** The dist hash the node computed; equal to the job's `distHash`. */
  distHash: string;
  /** True when the node already had this release, with this dist hash, and installed nothing. */
  alreadyInstalled: boolean;
}

/** A deploy job, with its input parsed. */
export interface PackageInstallJob extends PackageInstallInput {
  job: ExecutionJobEnvelope;
}

const signedDownloadUrlSchema = z
  .url({ protocol: /^https$/, message: 'must be an absolute https URL' })
  .refine((value) => {
    // zod runs this even after the URL check fails; an unparsable value is already reported.
    if (!URL.canParse(value)) {
      return true;
    }

    const url = new URL(value);
    return url.username === '' && url.password === '';
  }, { message: 'must not carry a username or password' });

export const packageInstallInputSchema = z
  .object({
    release: packageReleaseIdentitySchema,
    download: z
      .object({
        url: signedDownloadUrlSchema,
        expiresAt: timestampSchema.optional(),
      })
      .passthrough(),
    distHash: distHashSchema,
  })
  .passthrough();

export const packageInstallResultOutputSchema = z
  .object({
    release: packageReleaseIdentitySchema,
    distHash: distHashSchema,
    alreadyInstalled: z.boolean(),
  })
  .passthrough();

/** True when the job's capability is `{ kind: 'task', name: 'package.install' }`. */
export function isPackageInstallJob(job: ExecutionJobEnvelope): boolean {
  return job.payload.capability.kind === packageInstallCapability.kind && job.payload.capability.name === packageInstallCapability.name;
}

/**
 * Parses a deploy job's input, and checks that `packageAssignment`, when the job sets it, names the
 * same release. Throws an `EdgeConventionError` listing every problem; a node answers that with
 * `execution.failure` code `INVALID_INPUT` and `retryable: false`.
 *
 * Validation errors never include the download URL.
 */
export function parsePackageInstallJob(job: ExecutionJobEnvelope): PackageInstallJob {
  if (!isPackageInstallJob(job)) {
    throw new EdgeConventionError('package.install', [
      `capability must be {kind:'task', name:'package.install'}, not {kind:'${job.payload.capability.kind}', name:'${job.payload.capability.name}'}`,
    ]);
  }

  const parsed = packageInstallInputSchema.safeParse(job.payload.input);
  const issues = parsed.success ? [] : describeIssues(parsed.error, 'input');
  const assignment = job.payload.packageAssignment;

  if (parsed.success && assignment) {
    const { release } = parsed.data;

    if (assignment.packageId !== release.packageId || assignment.packageVersion !== release.packageVersion) {
      issues.push('packageAssignment: names another release than input.release');
    } else if (assignment.releaseId !== undefined && assignment.releaseId !== release.releaseId) {
      issues.push('packageAssignment.releaseId: differs from input.release.releaseId');
    }
  }

  if (!parsed.success || issues.length > 0) {
    throw new EdgeConventionError('package.install', issues);
  }

  return { job, ...(parsed.data as PackageInstallInput) };
}

// --- Tool jobs: capability { kind: 'tool', name } -------------------------------------------------

/**
 * The MCP tool-name rule: 1 to 128 characters, each an ASCII letter, digit, `_`, `-` or `.`.
 * A routing key carries a sanitized form of the name, so a node takes the name from the payload.
 */
export const toolNamePattern = /^[A-Za-z0-9_.-]{1,128}$/;

/** A tool job, with the fields the convention gives meaning to. */
export interface ToolJob {
  job: ExecutionJobEnvelope;
  /** The MCP tool name, exactly as the node's MCP server lists it. */
  name: string;
  /** The tool's `arguments`, passed to it unchanged. */
  arguments: Record<string, unknown>;
  timeoutMs: number;
  idempotencyKey: string;
}

/**
 * The `output` of the `execution.result` that answers a tool job: the MCP `CallToolResult`. A tool
 * that ran and reported an error is a result with `isError: true`, not an `execution.failure`.
 */
export interface ToolResultOutput {
  content: Array<{ type: string; [key: string]: unknown }>;
  structuredContent?: unknown;
  isError?: boolean;
  /** Absent, or `complete`: a job cannot answer `input_required`. */
  resultType?: 'complete';
  [key: string]: unknown;
}

export const toolResultOutputSchema = z
  .object({
    content: z.array(z.object({ type: z.string().min(1) }).passthrough()),
    structuredContent: z.unknown().optional(),
    isError: z.boolean().optional(),
    resultType: z.literal('complete').optional(),
  })
  .passthrough();

/** True when the job's capability kind is `tool`. */
export function isToolJob(job: ExecutionJobEnvelope): boolean {
  return job.payload.capability.kind === 'tool';
}

/**
 * Checks a tool job against the convention and returns its parts. Throws an `EdgeConventionError`
 * when the capability kind is not `tool` or the name breaks the MCP tool-name rule.
 */
export function parseToolJob(job: ExecutionJobEnvelope): ToolJob {
  const { capability } = job.payload;

  if (capability.kind !== 'tool') {
    throw new EdgeConventionError('tool', [`capability.kind must be 'tool', not '${capability.kind}'`]);
  }

  if (!toolNamePattern.test(capability.name)) {
    throw new EdgeConventionError('tool', [
      'capability.name: must be 1 to 128 characters, each an ASCII letter, digit, underscore, hyphen or dot',
    ]);
  }

  return {
    job,
    name: capability.name,
    arguments: job.payload.input,
    timeoutMs: job.payload.timeoutMs,
    idempotencyKey: job.payload.idempotencyKey,
  };
}

/** Validates a tool job's result output; throws an `EdgeConventionError` listing every problem. */
export function parseToolResultOutput(output: unknown): ToolResultOutput {
  const parsed = toolResultOutputSchema.safeParse(output);

  if (!parsed.success) {
    throw new EdgeConventionError('tool', describeIssues(parsed.error, 'output'));
  }

  return parsed.data as ToolResultOutput;
}
