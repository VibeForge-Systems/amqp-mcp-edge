import type {
  ExecutionJobEnvelope,
  JobAckMessage,
  JobFailureEvent,
  JobProgressEvent,
  JobResultEvent,
  NodeCapabilitySnapshot,
  NodeHeartbeatEvent,
  NodeHelloEvent,
} from '../index.js';
import { computeCapabilitiesDigest } from '../index.js';

export function createBaseEnvelope() {
  return {
    specVersion: '1.0' as const,
    timestamp: new Date().toISOString(),
    producer: {
      service: 'test-suite',
      version: '0.1.0',
    },
    compatibility: {
      channelVersion: '0.1.0',
      minConsumerVersion: '0.1.0',
      packageVersion: '0.1.0',
    },
  };
}

export function createExecutionJob(overrides: Partial<ExecutionJobEnvelope> = {}): ExecutionJobEnvelope {
  return {
    ...createBaseEnvelope(),
    messageType: 'execution.job',
    messageId: 'job-message-1',
    payload: {
      jobId: 'job-1',
      attemptId: 'attempt-1',
      idempotencyKey: 'job-1-attempt-1',
      timeoutMs: 30000,
      target: {
        siteId: 'site-a',
        nodeId: 'node-1',
      },
      createdBy: {
        controlPlane: 'example-control-plane',
      },
      capability: {
        kind: 'tool',
        name: 'search.documents',
      },
      input: {
        query: 'recent jobs',
      },
      ...(overrides.payload ?? {}),
    },
    ...overrides,
  };
}

export function createAckMessage(overrides: Partial<JobAckMessage> = {}): JobAckMessage {
  return {
    ...createBaseEnvelope(),
    messageType: 'execution.ack',
    messageId: 'ack-message-1',
    payload: {
      jobId: 'job-1',
      attemptId: 'attempt-1',
      siteId: 'site-a',
      nodeId: 'node-1',
      accepted: true,
      ...(overrides.payload ?? {}),
    },
    ...overrides,
  };
}

export function createProgressMessage(overrides: Partial<JobProgressEvent> = {}): JobProgressEvent {
  return {
    ...createBaseEnvelope(),
    messageType: 'execution.progress',
    messageId: 'progress-message-1',
    payload: {
      jobId: 'job-1',
      attemptId: 'attempt-1',
      siteId: 'site-a',
      nodeId: 'node-1',
      state: 'running',
      percent: 50,
      ...(overrides.payload ?? {}),
    },
    ...overrides,
  };
}

export function createResultMessage(overrides: Partial<JobResultEvent> = {}): JobResultEvent {
  return {
    ...createBaseEnvelope(),
    messageType: 'execution.result',
    messageId: 'result-message-1',
    payload: {
      jobId: 'job-1',
      attemptId: 'attempt-1',
      siteId: 'site-a',
      nodeId: 'node-1',
      completedAt: new Date().toISOString(),
      output: {
        ok: true,
      },
      ...(overrides.payload ?? {}),
    },
    ...overrides,
  };
}

export function createFailureMessage(overrides: Partial<JobFailureEvent> = {}): JobFailureEvent {
  return {
    ...createBaseEnvelope(),
    messageType: 'execution.failure',
    messageId: 'failure-message-1',
    payload: {
      jobId: 'job-1',
      attemptId: 'attempt-1',
      siteId: 'site-a',
      nodeId: 'node-1',
      failedAt: new Date().toISOString(),
      retryable: false,
      code: 'TOOL_ERROR',
      message: 'execution failed',
      ...(overrides.payload ?? {}),
    },
    ...overrides,
  };
}
export function createSnapshot(overrides: Partial<NodeCapabilitySnapshot> = {}): NodeCapabilitySnapshot {
  return {
    edgeLibrary: { name: '@vibeforge-systems/amqp-mcp-edge', version: '0.1.0' },
    runtimes: { node: '22.20.0' },
    packages: [
      {
        packageId: 'net-tools',
        packageVersion: '1.2.0',
        releaseId: 'rel-net-tools-1.2.0',
        distHash: `mcpod-dist-v2:${'a'.repeat(64)}`,
      },
    ],
    ...overrides,
  };
}

export function createHelloMessage(overrides: Partial<NodeHelloEvent> = {}): NodeHelloEvent {
  const capabilities = overrides.payload?.capabilities ?? createSnapshot();

  return {
    ...createBaseEnvelope(),
    messageType: 'node.hello',
    messageId: 'hello-message-1',
    payload: {
      siteId: 'site-a',
      nodeId: 'node-1',
      sessionId: 'session-1',
      sequence: 1,
      startedAt: '2026-11-14T10:00:00Z',
      heartbeatIntervalMs: 30000,
      capabilities,
      capabilitiesDigest: computeCapabilitiesDigest(capabilities),
      ...(overrides.payload ?? {}),
    },
    ...overrides,
  };
}

export function createHeartbeatMessage(overrides: Partial<NodeHeartbeatEvent> = {}): NodeHeartbeatEvent {
  return {
    ...createBaseEnvelope(),
    messageType: 'node.heartbeat',
    messageId: 'heartbeat-message-1',
    payload: {
      siteId: 'site-a',
      nodeId: 'node-1',
      sessionId: 'session-1',
      sequence: 2,
      heartbeatIntervalMs: 30000,
      capabilitiesDigest: computeCapabilitiesDigest(createSnapshot()),
      ...(overrides.payload ?? {}),
    },
    ...overrides,
  };
}

export const signedDownloadUrl = 'https://packages.example.com/releases/net-tools-1.2.0.tgz?expires=1763200000&signature=not-a-real-signature';

export function createPackageInstallJob(inputOverrides: Record<string, unknown> = {}, overrides: Partial<ExecutionJobEnvelope> = {}): ExecutionJobEnvelope {
  const base = createExecutionJob();

  return {
    ...base,
    messageId: 'install-message-1',
    ...overrides,
    payload: {
      ...base.payload,
      jobId: 'install-1',
      attemptId: 'install-1-attempt-1',
      idempotencyKey: 'install:rel-net-tools-1.2.0:site-a.node-1',
      timeoutMs: 120000,
      capability: { kind: 'task', name: 'package.install' },
      input: {
        release: { packageId: 'net-tools', packageVersion: '1.2.0', releaseId: 'rel-net-tools-1.2.0' },
        download: { url: signedDownloadUrl, expiresAt: '2026-11-15T10:00:00Z' },
        distHash: `mcpod-dist-v2:${'a'.repeat(64)}`,
        ...inputOverrides,
      },
      ...(overrides.payload ?? {}),
    },
  };
}
