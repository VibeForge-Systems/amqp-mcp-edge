import type {
  ExecutionJobEnvelope,
  JobAckMessage,
  JobFailureEvent,
  JobProgressEvent,
  JobResultEvent,
} from '../index.js';

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