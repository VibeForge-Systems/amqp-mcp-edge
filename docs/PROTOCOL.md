# Wire protocol

This page specifies what `@vibeforge-systems/amqp-mcp-edge` puts on the broker: the envelope every
message shares, the job and event payloads, the exchange and routing-key layout, and the
versioning rules. A control plane written in any language can interoperate with an edge worker
built on this library by following it.

The TypeScript interfaces below are the normative shapes; `src/contracts/` holds the runtime
validators that enforce them.

## Message Envelope Specification

Base envelope:

```ts
export interface BaseEdgeEnvelope<TType extends string, TPayload> {
  specVersion: '1.0';
  messageType: TType;
  messageId: string;
  timestamp: string;
  producer: {
    service: string;
    version: string;
    instanceId?: string;
  };
  compatibility: CompatibilityMetadata;
  payload: TPayload;
}
```

Compatibility metadata:

```ts
export interface CompatibilityMetadata {
  channelVersion: string;
  minConsumerVersion?: string;
  minProducerVersion?: string;
  serverFamily?: string;
  serverVersion?: string;
  packageVersion?: string;
}
```

Supported message types:
- `execution.job`
- `execution.ack`
- `execution.progress`
- `execution.result`
- `execution.failure`

## Job Contract

Execution job payload:

```ts
export interface ExecutionJobPayload {
  jobId: string;
  attemptId: string;
  idempotencyKey: string;
  attempt?: ExecutionAttemptMetadata;
  target: TargetSelector;
  timeoutMs: number;
  createdBy: {
    controlPlane: string;
    actorId?: string;
  };
  capability: {
    kind: 'tool' | 'resource' | 'prompt' | 'task';
    name: string;
    version?: string;
  };
  input: Record<string, unknown>;
  packageAssignment?: {
    packageId: string;
    packageVersion: string;
    releaseId?: string;
  };
}

export type ExecutionJobEnvelope = BaseEdgeEnvelope<'execution.job', ExecutionJobPayload>;
```

Target selector:

```ts
export interface TargetSelector {
  siteId?: string;
  nodeId?: string;
  nodeGroup?: string;
  capabilityTags?: string[];
}
```

## Event Contracts

Ack event:

```ts
export interface JobAckPayload {
  jobId: string;
  attemptId: string;
  siteId?: string;
  nodeId: string;
  accepted: boolean;
  reason?: string;
}

export type JobAckMessage = BaseEdgeEnvelope<'execution.ack', JobAckPayload>;
```

Progress event:

```ts
export interface JobProgressPayload {
  jobId: string;
  attemptId: string;
  siteId?: string;
  nodeId: string;
  state: 'received' | 'started' | 'running' | 'waiting' | 'finishing';
  percent?: number;
  message?: string;
  metrics?: Record<string, number>;
}

export type JobProgressEvent = BaseEdgeEnvelope<'execution.progress', JobProgressPayload>;
```

Result event:

```ts
export interface JobResultPayload {
  jobId: string;
  attemptId: string;
  siteId?: string;
  nodeId: string;
  completedAt: string;
  output: Record<string, unknown>;
  artifacts?: Array<{
    name: string;
    contentType: string;
    uri?: string;
    inlineData?: string;
  }>;
}

export type JobResultEvent = BaseEdgeEnvelope<'execution.result', JobResultPayload>;
```

Failure event:

```ts
export interface JobFailurePayload {
  jobId: string;
  attemptId: string;
  siteId?: string;
  nodeId: string;
  failedAt: string;
  retryable: boolean;
  code: string;
  message: string;
  details?: Record<string, unknown>;
}

export type JobFailureEvent = BaseEdgeEnvelope<'execution.failure', JobFailurePayload>;
```

## Routing And Namespace Rules

Recommended exchange names:
- jobs exchange: `mcp.edge.jobs`
- events exchange: `mcp.edge.events`
- dead-letter exchange: `mcp.edge.dlx`

Recommended routing key patterns:
- job dispatch: `edge.job.<siteId|any>.<nodeId|group|any>.<capabilityKind>.<capabilityName>`
- ack: `edge.event.ack.<siteId>.<nodeId>`
- progress: `edge.event.progress.<siteId>.<nodeId>`
- result: `edge.event.result.<siteId>.<nodeId>`
- failure: `edge.event.failure.<siteId>.<nodeId>`

Rules:
- job and event routing must remain separate from [amqp-mcp-transport](https://github.com/VibeForge-Systems/amqp-mcp-transport) routing.
- message contracts must never depend on reply queues.
- queue naming should support per-site and per-node isolation.

## Versioning And Compatibility

Rules:
- `specVersion` changes only for breaking envelope changes.
- additive fields are minor-version compatible.
- consumers must ignore unknown additive fields.
- breaking semantic changes require a new `specVersion`.
- publishers may require minimum consumer versions with `compatibility.minConsumerVersion`.

Recommended release policy:
- semver for package releases
- changelog entry for every contract-affecting change
- explicit migration notes for every breaking `specVersion` change

Runtime enforcement notes:
- compatibility checks are enforced on consumer inbound envelopes before handler execution
- compatibility checks are enforced on publisher outbound envelopes before publish
- a consumer configured without `runtimeConsumerVersion` rejects any job that sets `compatibility.minConsumerVersion`

