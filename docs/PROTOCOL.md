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

## Timestamp Format

These fields hold timestamps:

| Field | Where |
|:---|:---|
| `timestamp` | every envelope |
| `payload.attempt.firstAttemptAt`, `payload.attempt.previousAttemptAt` | `execution.job`, optional |
| `payload.completedAt` | `execution.result` |
| `payload.failedAt` | `execution.failure` |

Each is an RFC 3339 date-time, and the validators accept only this form:

- **Seconds are required.** Any number of fractional-second digits may follow them:
  `2026-11-12T10:00:00Z` and `2026-11-12T10:00:00.123456Z` are both valid.
- **The offset is `Z` or `±hh:mm`**, as in `2026-11-12T10:00:00+05:30`. An offset is required.
- **`T` and `Z` are upper-case.**
- **Rejected:** minute precision (`2026-11-12T10:00Z`), an offset without the colon
  (`2026-11-12T10:00:00+0530`), an hours-only offset (`2026-11-12T10:00:00+05`), a time with no
  offset (`2026-11-12T10:00:00`), lower-case `t` or `z`, and a leap second
  (`2026-11-12T10:00:60Z`), which RFC 3339 allows but the validators do not.

JavaScript's `new Date().toISOString()` produces a valid value, and so does Python's
`datetime.now(timezone.utc).isoformat()`. A job with an invalid timestamp fails validation, and the
consumer rejects it without requeue (see [Broker Topology](#broker-topology)). A publisher refuses
to send an event with an invalid timestamp.

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

`<siteId>` and `<nodeId>` in routing keys and queue names are sanitized: lowercased, with every
character outside `a-z 0-9 - _` replaced by `-`. Site `Site A` and node `node.1` become `site-a`
and `node-1`. Permission patterns (below) must use the sanitized form.

## Broker Topology

The control plane owns the broker topology. Edge nodes run in networks the control plane does not
control and share one broker, so in production a node never declares, binds or deletes anything:
it runs with `topologyMode: 'passive'` and a broker user that has no configure permission (see
[Least-Privilege Permissions](#least-privilege-permissions)). The control plane, or the operator
when self-hosting, declares the following before a node starts.

Shared, once per vhost:

| Entity | Type | Declaration |
|:---|:---|:---|
| `mcp.edge.jobs` | exchange | topic, durable |
| `mcp.edge.events` | exchange | topic, durable |
| `mcp.edge.dlx` | exchange | topic, durable |
| a dead-letter queue, such as `mcp.edge.dlq` | queue | durable, bound to `mcp.edge.dlx` with `#` |

Per node:

| Entity | Declaration |
|:---|:---|
| queue `mcp.edge.consumer.<siteId>.<nodeId>` | durable (classic or quorum), argument `x-dead-letter-exchange: mcp.edge.dlx` |
| its bindings on `mcp.edge.jobs` | `edge.job.<siteId>.<nodeId>.*.*`, `edge.job.<siteId>.any.*.*`, `edge.job.any.<nodeId>.*.*`, `edge.job.any.any.*.*` |
| a broker user for the node | with the permissions below |

A node in a group (`nodeGroup`) also binds `edge.job.<siteId>.<nodeGroup>.*.*` and
`edge.job.any.<nodeGroup>.*.*`. `buildConsumerQueueName` and `buildJobBindingKeys` compute the
queue name and binding keys from a node's config, so a control plane written in TypeScript can
reuse them.

Dead-lettering: a node rejects a job it cannot parse or validate with `requeue: false`, and the
broker moves it to `mcp.edge.dlx` with its original routing key; the node needs no permission for
this. Without a queue bound to `mcp.edge.dlx`, rejected jobs are dropped. Jobs addressed to `any`
keep `any` in their routing key, so bind the dead-letter queue with `#`, or set
`x-dead-letter-routing-key` on each node queue if dead letters must be told apart by node.

A node in passive mode checks, with passive declares, that its queue and `mcp.edge.events` exist,
once at startup and again once after each reconnect. If either is missing, startup fails with an
`EdgeTopologyError` naming it, and nothing is created. A consumer whose queue has gone after a
reconnect stops, and its `health()` reports `consumerActive: false` with the reason in
`details.topologyError`. A node does not check `mcp.edge.jobs` or `mcp.edge.dlx`: it holds no
permission on them, and RabbitMQ 4 refuses a passive declare from a user with no permission on the
name.

The default `topologyMode: 'declare'` declares all of the above for the node itself (except the
dead-letter queue and the node user), which needs the configure permission. It is meant for local
development against a broker you own.

## Least-Privilege Permissions

Each node gets its own broker user, with this permission set in its vhost:

| Permission | Pattern | What it allows |
|:---|:---|:---|
| configure | `^$` | nothing: no declare, delete or bind of any exchange or queue |
| write | `^mcp\.edge\.events$` | publishing events |
| read | `^mcp\.edge\.consumer\.<siteId>\.<nodeId>$` | consuming its own queue |
| topic write on `mcp.edge.events` | `^edge\.event\.[a-z]+\.<siteId>\.<nodeId>$` | publishing only under its own routing keys |
| topic read on `mcp.edge.events` | `^$` | nothing |

With RabbitMQ's `rabbitmqctl`, for site `site-a` and node `node-1`:

```bash
rabbitmqctl add_user 'node-1@site-a' '<password>'
rabbitmqctl set_permissions -p <vhost> 'node-1@site-a' \
  '^$' '^mcp\.edge\.events$' '^mcp\.edge\.consumer\.site-a\.node-1$'
rabbitmqctl set_topic_permissions -p <vhost> 'node-1@site-a' \
  mcp.edge.events '^edge\.event\.[a-z]+\.site-a\.node-1$' '^$'
```

What this rules out, verified on RabbitMQ 4.4.0 by the integration suite:

- **No deletes.** Deleting an exchange or a queue needs the configure permission, so a node cannot
  delete `mcp.edge.jobs`, `mcp.edge.events`, `mcp.edge.dlx` or its own queue; the broker answers
  `ACCESS_REFUSED`. With configure on the shared exchanges, any node could delete the exchanges
  every other node depends on.
- **No new bindings.** Binding a queue needs write on the queue and read on the exchange, so a node
  cannot bind its queue to more of `mcp.edge.jobs`.
- **No active declares.** Declaring an exchange or queue, even with arguments identical to the
  existing ones, needs configure. That is why passive mode exists.

Passive declares (`checkExchange` and `checkQueue` in amqplib) need no configure permission.
RabbitMQ 4 (4.4.0 tested) does require at least one of configure, write or read on the name, and
refuses the check with `ACCESS_REFUSED` otherwise, even for a name that does not exist; RabbitMQ
3.12.1 required none. The permission set above covers exactly what passive mode checks: the node
queue (read) and `mcp.edge.events` (write).

Two rules for whoever writes the permissions:

- **Never grant a node read on an exchange.** Binding one exchange to another needs read on the
  source and write on the destination, and no configure. A node with read on `mcp.edge.jobs` and
  write on `mcp.edge.events` can bind `mcp.edge.events` to `mcp.edge.jobs` and copy every node's
  jobs onto the events exchange; topic permissions do not stop it (observed on RabbitMQ 3.12.1 and
  4.4.0).
- **Permission patterns match names, not types.** The read pattern for a node queue would also
  match an exchange with the same name, and the write pattern a queue named `mcp.edge.events`.
  Keep queue and exchange names distinct.

## Node Identity

The AMQP `user_id` message property is the node's identity on the wire. Payload fields such as
`nodeId` and `siteId`, and the routing key, are claims the node makes; `user_id` is checked by the
broker.

- **The library sets it on every event.** Every ack, progress, result and failure event carries
  `user_id` equal to the broker username the node authenticated with: the `userId` config field
  if set, otherwise the URL-decoded username in `amqpUrl`, otherwise `guest` (what amqplib logs in
  with when the URL has no credentials).
- **The broker validates it.** RabbitMQ compares `user_id`, when it is present, with the
  connection's user. On a mismatch it does not route the message and closes the channel with
  `406 PRECONDITION_FAILED` ("user_id property set to '…' but authenticated user was '…'").
- **Consumers must reject events that lack it.** RabbitMQ does not check a message without
  `user_id`; such a message is delivered with no identity. A consumer of `mcp.edge.events` must
  reject (and should dead-letter or log) any event without `user_id`. It should also map `user_id`
  to the site and node it provisioned for that user, and reject events whose routing key or
  `payload.siteId` / `payload.nodeId` belong to another node.
- **Never give a node user the `impersonator` tag.** A user with that tag may set any `user_id`.

Events published by versions of this library before `user_id` was added carry none. Upgrade the
edge nodes before a consumer starts enforcing the rule above.

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

