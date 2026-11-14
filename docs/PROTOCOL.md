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

| Message type | Direction | Exchange | Defined in |
|:---|:---|:---|:---|
| `execution.job` | control plane → node | `mcp.edge.jobs` | [Job Contract](#job-contract) |
| `execution.ack` | node → control plane | `mcp.edge.events` | [Event Contracts](#event-contracts) |
| `execution.progress` | node → control plane | `mcp.edge.events` | [Event Contracts](#event-contracts) |
| `execution.result` | node → control plane | `mcp.edge.events` | [Event Contracts](#event-contracts) |
| `execution.failure` | node → control plane | `mcp.edge.events` | [Event Contracts](#event-contracts) |
| `node.hello` | node → control plane | `mcp.edge.events` | [Node Lifecycle Messages](#node-lifecycle-messages) |
| `node.heartbeat` | node → control plane | `mcp.edge.events` | [Node Lifecycle Messages](#node-lifecycle-messages) |

All of them carry `specVersion: '1.0'`. How a consumer treats a message type it does not know is
in [Versioning And Compatibility](#versioning-and-compatibility).

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

## Node Lifecycle Messages

A node announces itself with `node.hello` and stays visible with `node.heartbeat`. Both go to
`mcp.edge.events`, like the execution events, and carry the AMQP `user_id` that identifies the node
(see [Node Identity](#node-identity)). The library validates and publishes them
(`publisher.publishNodeHello`, `publisher.publishNodeHeartbeat`); when to send them is the worker's
job, following the rules below.

Capability snapshot, what a node can run:

```ts
export interface NodeCapabilitySnapshot {
  edgeLibrary: {
    name: string;       // '@vibeforge-systems/amqp-mcp-edge'
    version: string;    // this library's version, such as '0.1.0'
  };
  runtimes: Record<string, string>; // name to version, such as { node: '22.20.0' }; at least one
  packages: InstalledPackage[];     // may be empty
}

export interface InstalledPackage {
  packageId: string;
  packageVersion: string;
  releaseId?: string;   // the release the control plane installed it from, if it did
  distHash: string;     // the dist hash the node computed over what it installed; see "Deploy jobs"
}
```

Hello, published once after every connection to the broker, the first one included:

```ts
export interface NodeHelloPayload {
  siteId: string;
  nodeId: string;
  sessionId: string;            // one run of the node process: kept across reconnects, not restarts
  sequence: number;             // positive safe integer; see the rules below
  startedAt: string;            // when this session started (RFC 3339)
  heartbeatIntervalMs: number;  // positive safe integer
  capabilities: NodeCapabilitySnapshot;
  capabilitiesDigest: string;   // the digest of capabilities
}

export type NodeHelloEvent = BaseEdgeEnvelope<'node.hello', NodeHelloPayload>;
```

Heartbeat, published every `heartbeatIntervalMs` while the node is connected:

```ts
export interface NodeHeartbeatPayload {
  siteId: string;
  nodeId: string;
  sessionId: string;
  sequence: number;
  heartbeatIntervalMs: number;
  capabilitiesDigest: string;               // the digest of the node's current snapshot
  capabilities?: NodeCapabilitySnapshot;    // the snapshot itself; see the rules below
}

export type NodeHeartbeatEvent = BaseEdgeEnvelope<'node.heartbeat', NodeHeartbeatPayload>;
```

Unlike the execution events, both require `siteId`: a node is identified by its site and node
together.

### Capability digest

`capabilitiesDigest` is `sha256:` followed by the 64 lowercase hex digits of the SHA-256 of the
snapshot's canonical JSON, as [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785) (JCS) defines
it: no whitespace, object members sorted by the UTF-16 code units of their names, and strings and
numbers written as ECMAScript's `JSON.stringify` writes them. The digest covers every member the
snapshot carries, unknown ones included. Arrays keep their order, so `packages` listed in another
order give another digest; `createCapabilitySnapshot` sorts them by `packageId`, then
`packageVersion`, and a node should list them in that order.

`computeCapabilitiesDigest` computes it, and the validators reject a `node.hello` or
`node.heartbeat` that carries `capabilities` with a digest that does not match them. A control
plane may compare digests without computing them.

Test vector. This snapshot

```json
{
  "runtimes": { "node": "22.20.0" },
  "packages": [
    {
      "releaseId": "rel-1",
      "packageVersion": "1.2.0",
      "packageId": "net-tools",
      "distHash": "mcpod-dist-v2:0000000000000000000000000000000000000000000000000000000000000000"
    }
  ],
  "edgeLibrary": { "version": "0.1.0", "name": "@vibeforge-systems/amqp-mcp-edge" }
}
```

has the canonical form

```text
{"edgeLibrary":{"name":"@vibeforge-systems/amqp-mcp-edge","version":"0.1.0"},"packages":[{"distHash":"mcpod-dist-v2:0000000000000000000000000000000000000000000000000000000000000000","packageId":"net-tools","packageVersion":"1.2.0","releaseId":"rel-1"}],"runtimes":{"node":"22.20.0"}}
```

and the digest
`sha256:c50b81fdd0a559330746ba1d3a830ad7f6a2b20b934ec7c90c40951d746530d2`.

### Sending rules

A node:

- **Sends `node.hello` after every connection**, before any other event on that connection.
  A reconnect keeps the `sessionId`; a process restart starts a new one, with a new `startedAt`.
- **Numbers its lifecycle messages.** `sequence` is 1 on the first `node.hello` of a session and
  increases by at least 1 with every `node.hello` and `node.heartbeat` after it, across reconnects.
- **Sends `node.heartbeat` every `heartbeatIntervalMs`** while connected, with the digest of its
  current snapshot.
- **Sends the snapshot when it changes.** A heartbeat must carry `capabilities` when the snapshot
  differs from the one in the node's previous lifecycle message of the session, for example after
  a `package.install` job. It may carry it at any time, and should carry it at least every tenth
  heartbeat, so a control plane that lost its record catches up.

A control plane:

- **Keeps, per node, the latest `sessionId`, `sequence`, snapshot and digest.** It ignores a
  lifecycle message whose `sequence` is not greater than the last one it accepted for that session.
  A new `sessionId` means the node restarted: its in-flight jobs will not complete.
- **Treats a node as offline** when no lifecycle message of its current session has arrived for
  three times its `heartbeatIntervalMs`.
- **Treats the node's capabilities as unknown** when a heartbeat's digest differs from the stored
  one and the heartbeat carries no snapshot, until a message that carries one arrives.

## Job Conventions

The job contract leaves `capability` and `input` open. These conventions give them a meaning for
the two kinds of work a control plane sends today: running an MCP tool, and installing a package.
A job is in a convention when its `capability` selects it; `parseToolJob` and
`parsePackageInstallJob` check one, and throw an `EdgeConventionError` listing what is wrong.
`validateEnvelope` does not apply them, so a malformed job reaches the handler, which can answer
it with an `execution.failure` instead of leaving it to the dead-letter queue.

### Tool jobs

`capability: { kind: 'tool', name }` runs one MCP tool.

| Field | Meaning |
|:---|:---|
| `capability.name` | The MCP tool name, exactly as the node's MCP server lists it: 1 to 128 characters, each an ASCII letter, digit, `_`, `-` or `.`, as the MCP tool-name rule says. The routing key carries a sanitized form (`search.documents` becomes `search-documents`), so a node always takes the name from the payload. |
| `capability.version` | Optional. When set, a node that cannot run that version fails the job with `UNSUPPORTED_CAPABILITY`. |
| `input` | The tool's `arguments` object, passed to the tool unchanged. The tool validates it against its input schema; invalid arguments are `INVALID_INPUT`. |
| `timeoutMs` | The longest the tool may run, measured from when the node starts it. Past that, the node stops it and publishes `execution.failure` with code `TIMEOUT`. |
| `idempotencyKey` | The same on every attempt of one logical call; each attempt has a new `attemptId`. A node may remember completed keys and answer a repeat with the earlier outcome instead of running the tool again. |
| `packageAssignment` | Optional. When set, the node runs the tool from that package release, or fails the job with `UNSUPPORTED_CAPABILITY`. |

The `execution.result` output is the tool's MCP `CallToolResult`: a `content` array of content
blocks, and optionally `structuredContent` (any JSON value) and `isError`. A tool that ran and
reported an error is an `execution.result` with `isError: true`, not an `execution.failure`.
`execution.failure` means the tool did not produce a result: unknown tool, invalid input, timeout,
or a crash. A job cannot answer the questions of an `input_required` result, so a node that gets
one fails the job with `INPUT_REQUIRED`; `resultType`, if present, is `complete`.
`parseToolResultOutput` validates the output.

`retryable` on a tool failure is the node's judgment of whether running the same call again is
safe. A node should set it to `true` only for tools annotated `readOnlyHint` or `idempotentHint`,
and for failures that happened before the tool started.

### Deploy jobs (`package.install`)

`capability: { kind: 'task', name: 'package.install' }` installs one package release on the node.

```ts
export interface PackageInstallInput {
  release: {
    packageId: string;
    packageVersion: string;
    releaseId: string;
  };
  download: {
    url: string;          // a signed https URL for the release archive
    expiresAt?: string;   // RFC 3339, when the URL stops working, if known
  };
  distHash: string;       // the dist hash the node must compute before installing anything
}
```

- **The release** is identified by all three fields. `packageAssignment`, if the job sets it, must
  name the same release.
- **The URL** is `https`, absolute, and carries no username or password. It is signed, so it is a
  credential for as long as it is valid: a node never logs it and never copies it into an event.
  An expired or refused URL is `PACKAGE_URL_EXPIRED`, which the control plane answers with a new
  job carrying a fresh URL.
- **The dist hash** is an algorithm tag, a colon, and 64 or 128 lowercase hex digits, with nothing
  before or after it, such as `mcpod-dist-v2:<64 hex>`. The package format defines the tag and how
  the value is computed over the archive's contents; this protocol carries it and compares it as
  an exact string. A node that does not implement the tag fails the job with
  `PACKAGE_HASH_UNSUPPORTED`. A node computes the hash over what it downloaded before it loads,
  runs or installs anything from it; on a mismatch it installs nothing and fails the job with
  `PACKAGE_HASH_MISMATCH`.
- **Targeting.** A deploy job names one node (`target.siteId` and `target.nodeId`) and routes as
  `edge.job.<siteId>.<nodeId>.task.package-install`.
- **`timeoutMs`** covers the download, the verification and the installation.
- **Idempotency.** Installing a release the node already has, with the same dist hash, succeeds
  without installing anything, and reports `alreadyInstalled: true`. A suitable `idempotencyKey`
  is `install:<releaseId>:<siteId>.<nodeId>`.

The `execution.result` output (`packageInstallResultOutputSchema`):

```ts
export interface PackageInstallResultOutput {
  release: { packageId: string; packageVersion: string; releaseId: string };
  distHash: string;           // the dist hash the node computed
  alreadyInstalled: boolean;
}
```

After a successful install, the node's next lifecycle message carries the new snapshot, with the
release in `packages`.

### Failure codes

`execution.failure` `code` values with a standard meaning, exported as `edgeFailureCodes`. A node
may use other codes as well.

| Code | Meaning | `retryable` |
|:---|:---|:---|
| `INVALID_INPUT` | The input breaks the capability's input contract, or the job breaks its convention | `false` |
| `UNSUPPORTED_CAPABILITY` | The node does not provide the capability, or not the version or release asked for | `false` |
| `TIMEOUT` | The job ran past `timeoutMs` and was stopped | see [Tool jobs](#tool-jobs) |
| `INPUT_REQUIRED` | The tool answered `input_required` | `false` |
| `HANDLER_ERROR` | Anything else that stopped the handler | the node's judgment |
| `DELIVERY_ATTEMPTS_EXHAUSTED` | Published by the library: the handler threw on the last of `maxDeliveryAttempts` deliveries, and the job was dead-lettered; see [Delivery Attempts](#delivery-attempts) | `false` |
| `PACKAGE_DOWNLOAD_FAILED` | The archive could not be downloaded | `true` |
| `PACKAGE_URL_EXPIRED` | The signed URL had expired, or the store refused it | `true`, with a new URL |
| `PACKAGE_HASH_UNSUPPORTED` | The node does not implement the dist hash's algorithm tag | `false` |
| `PACKAGE_HASH_MISMATCH` | The computed dist hash differs from the expected one; nothing was installed | `false` |
| `PACKAGE_INSTALL_FAILED` | Downloaded and verified, but the installation failed | `false` |

`retryable: true` tells the control plane that a new attempt (same `idempotencyKey`, new
`attemptId`) may succeed. `details` must not contain the download URL or any other credential.

### Reserved capability names

Task names that start with `package.` or `node.` are reserved for conventions defined here. A node
that receives a reserved task name it does not implement fails the job with
`UNSUPPORTED_CAPABILITY`. Node control actions (validating installed packages, retiring a node,
changing its configuration) will be defined as `node.*` tasks, so they reuse the job's
acknowledgement, timeout, idempotency and dead-lettering instead of needing a message type and
routing of their own.

## Delivery Attempts

A job whose handler throws is redelivered a bounded number of times, then reported and
dead-lettered, so one bad job cannot hold a node's queue.

- **`deliveryAttempt`.** Each delivery reaches the handler with `deliveryAttempt`, counted from 1:
  the first delivery, plus every requeue this consumer made of the same job attempt (the same
  `messageId` and `attemptId`), plus the returns the broker counted in `x-delivery-count`. A job
  the broker marks redelivered is at least attempt 2, even when this process never saw it before.
- **`maxDeliveryAttempts`** (default 2). When the handler throws and `deliveryAttempt` is below
  it, the consumer rejects the job with `requeue: true`. When the handler throws on attempt
  `maxDeliveryAttempts`, the consumer publishes an `execution.failure` and then rejects the job
  without requeue, which dead-letters it. The failure has code `DELIVERY_ATTEMPTS_EXHAUSTED`,
  `retryable: false`, the job's `jobId` and `attemptId`, the node's `siteId` and `nodeId`, and
  `details: { deliveryAttempt, maxDeliveryAttempts, handlerError }`.
- **`handlerError` carries a code, not the handler's message.** It is
  `{ code: 'HANDLER_ERROR', name?, description? }`. `name` is the error's class name, such as
  `TypeError`, sent only when it is a plain identifier of at most 64 characters. The error's
  message is never sent by default, because error messages often carry connection strings,
  signed URLs, file paths or other secrets, and a node may run on infrastructure its operator does
  not control. A control plane must not expect error text. A node opts in with the
  `describeHandlerError` option, a function whose result is sent as `description`, cut to 1000
  characters; the node is then responsible for keeping credentials out of it.
- **The job is dead-lettered even if that failure cannot be published**, for example when the
  config has no `nodeId` or the broker refuses the event: requeueing it would restart the loop.
  The consumer's `health()` then reports `failureReportErrors` and `lastFailureReportError`.
- **A handler that settles the job itself keeps its outcome.** If it acks or nacks before it
  throws, the consumer does nothing more. A handler's own `nack(delivery, { requeue: true })`
  counts toward the next `deliveryAttempt` but is not limited by `maxDeliveryAttempts`; such a
  handler should check `deliveryAttempt` itself.
- **A new attempt starts over.** The control plane retries a job as a new message with a new
  `attemptId`, whose first delivery is attempt 1 again.

The count is kept by the consumer, in memory, because the broker does not keep it: RabbitMQ 4.4
sets no `x-delivery-count` when a job is rejected with `requeue: true`, on classic or quorum
queues, and classic queues never set it. A quorum queue does count a delivery whose channel closed
while it was unacked, which is what happens when the node process crashes mid-job; the consumer
adds that count, and the queue's `x-delivery-limit` dead-letters a job that keeps crashing the
node (reason `delivery_limit`), a case the consumer cannot catch. Declare node queues as quorum
queues with a small `x-delivery-limit`, such as 5, for that protection.

## Timestamp Format

These fields hold timestamps:

| Field | Where |
|:---|:---|
| `timestamp` | every envelope |
| `payload.attempt.firstAttemptAt`, `payload.attempt.previousAttemptAt` | `execution.job`, optional |
| `payload.completedAt` | `execution.result` |
| `payload.failedAt` | `execution.failure` |
| `payload.startedAt` | `node.hello` |
| `payload.input.download.expiresAt` | a `package.install` job, optional; checked by `parsePackageInstallJob` |

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
- hello: `edge.event.hello.<siteId>.<nodeId>`
- heartbeat: `edge.event.heartbeat.<siteId>.<nodeId>`

Every event routing key has five segments, `edge.event.<type>.<siteId>.<nodeId>`;
`parseEventRoutingKey` splits one, and `buildNodeKey` gives the `<siteId>.<nodeId>` pair.

Rules:
- job and event routing must remain separate from [amqp-mcp-transport](https://github.com/VibeForge-Systems/amqp-mcp-transport) routing.
- message contracts must never depend on reply queues.
- queue naming should support per-site and per-node isolation.

`<siteId>` and `<nodeId>` in routing keys and queue names are sanitized: lowercased, with every
character outside `a-z 0-9 - _` replaced by `-`. Site `Site A` and node `node.1` become `site-a`
and `node-1`. Permission patterns (below) must use the sanitized form.

## Publishing Guarantees

A node publishes every event as a persistent message on a confirm channel, and a publish
resolves only once the broker confirms it. For a persistent message routed to durable queues,
RabbitMQ confirms after every one of those queues has taken it. A publish rejects with an
`EdgePublishError` when the broker nacks the event (`reason: 'nacked'`), when the channel closes
before the confirm (`'channel-closed'`), or when no confirm arrives within `confirmTimeoutMs`
(`'timeout'`).

- **A node acks a job only after the events that report it are confirmed.** Publish the result
  (or failure), await it, then ack. If the publish rejects, do not ack: the job is redelivered, or
  dead-lettered after its last attempt (see [Delivery Attempts](#delivery-attempts)), instead of
  being acked with an outcome the control plane never received.
- **A rejected publish may still have been delivered.** A nack means the broker did not take
  responsibility for the event everywhere it was routed; with several queues bound, some may
  have it. A timeout or a closed channel leaves the same doubt. A node that publishes again
  produces a duplicate, so a consumer of `mcp.edge.events` deduplicates by `messageId`, and by
  `jobId` and `attemptId` for outcomes.
- **An event no binding matches is confirmed and dropped.** The library does not publish with
  `mandatory`, so the platform binds its event queues before any node starts (see
  [Broker Topology](#broker-topology)).

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
| queue `mcp.edge.consumer.<siteId>.<nodeId>` | durable (quorum recommended, with `x-delivery-limit`; see [Delivery Attempts](#delivery-attempts)), argument `x-dead-letter-exchange: mcp.edge.dlx` |
| its bindings on `mcp.edge.jobs` | `edge.job.<siteId>.<nodeId>.*.*`, `edge.job.<siteId>.any.*.*`, `edge.job.any.<nodeId>.*.*`, `edge.job.any.any.*.*` |
| a broker user for the node | with the permissions below |

A node in a group (`nodeGroup`) also binds `edge.job.<siteId>.<nodeGroup>.*.*` and
`edge.job.any.<nodeGroup>.*.*`. `buildConsumerQueueName` and `buildJobBindingKeys` compute the
queue name and binding keys from a node's config, so a control plane written in TypeScript can
reuse them.

Jobs a node rejects go to the dead-letter queue, which the platform declares; see
[Dead-Letter Queue](#dead-letter-queue).

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

### Dead-Letter Queue

**The platform owns the dead-letter queue.** The control plane, or the operator when
self-hosting, declares it and binds it to `mcp.edge.dlx` before any node starts, and monitors its
depth. The library never declares or binds it, in either topology mode: declaring a queue needs the
configure permission, which a node does not have. In declare mode, which is for local development,
the library declares `mcp.edge.dlx` and points the node queue at it, but binds no queue to it, so
rejected jobs are dropped unless you bind one yourself.

What a self-hosting operator declares:

| Entity | Declaration |
|:---|:---|
| exchange `mcp.edge.dlx` | topic, durable |
| a queue such as `mcp.edge.dlq` | durable (quorum recommended), bound to `mcp.edge.dlx` with `#` |
| on every node queue | the argument `x-dead-letter-exchange: mcp.edge.dlx` |

A node rejects a job with `requeue: false`, and the broker moves it to `mcp.edge.dlx`; the node
needs no permission on the exchange or the queue for this. A job is dead-lettered when it:

- is larger than the node's `maxMessageSizeBytes`;
- is not JSON, fails envelope validation, or is not an `execution.job`;
- fails the compatibility check (`compatibility.minConsumerVersion`);
- made the node's handler throw on its last delivery attempt; the node first publishes an
  `execution.failure` with code `DELIVERY_ATTEMPTS_EXHAUSTED` (see
  [Delivery Attempts](#delivery-attempts));
- is rejected by the node's handler with `nack(delivery, { requeue: false })`;
- reached a quorum queue's `x-delivery-limit`; the `x-death` reason is then `delivery_limit`.

The broker keeps the job's routing key and adds an `x-death` header naming the node queue, the
reason (`rejected`) and the count. Jobs addressed to `any` keep `any` in their routing key, so bind
the dead-letter queue with `#`, or set `x-dead-letter-routing-key` on each node queue if dead
letters must be told apart by node. Without a queue bound to `mcp.edge.dlx`, the broker drops
dead-lettered jobs silently, which is why the platform declares it before any node starts.

Bound the queue's growth with a policy (for example `max-length` or `message-ttl`), and alert on
its depth: a job in it was never run to completion, and for the first three cases and the last
case above the node published no event about it.

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

- **The library sets it on every event.** Every ack, progress, result, failure, hello and
  heartbeat event carries `user_id` equal to the broker username the node authenticated with: the
  `userId` config field if set, otherwise the URL-decoded username in `amqpUrl`, otherwise `guest`
  (what amqplib logs in with when the URL has no credentials).
- **The broker validates it.** RabbitMQ compares `user_id`, when it is present, with the
  connection's user. On a mismatch it does not route the message and closes the channel with
  `406 PRECONDITION_FAILED` ("user_id property set to '…' but authenticated user was '…'").
- **Consumers must reject events that lack it.** RabbitMQ does not check a message without
  `user_id`; such a message is delivered with no identity. A consumer of `mcp.edge.events` must
  reject (and should dead-letter or log) any event without `user_id`. It should also map `user_id`
  to the site and node it provisioned for that user, and reject events whose routing key or
  `payload.siteId` / `payload.nodeId` belong to another node.
- **Never give a node user the `impersonator` tag.** A user with that tag may set any `user_id`.

A node is known on the wire by its key, `<siteId>.<nodeId>` in sanitized form, which is also the
last two segments of its event routing keys and of its queue name. The control plane records, when
it provisions a node, the broker username it created and the node's site and node IDs, and maps
`user_id` to that record; it does not parse the key out of the username. On every event,
`checkEventIdentity(event, provisioned)` applies the rules above and returns the reasons to reject
it: no `user_id`, a `user_id` other than the provisioned user, a routing key other than the one
`buildRoutingKey` gives for the provisioned node, or a `payload.nodeId` or `payload.siteId` that
sanitizes to another node. Site and node IDs that are already in sanitized form avoid two IDs
mapping to the same key.

Events published by versions of this library before `user_id` was added carry none. Upgrade the
edge nodes before a consumer starts enforcing the rule above.

## Versioning And Compatibility

Rules:
- `specVersion` changes only for breaking envelope changes.
- additive fields are minor-version compatible.
- consumers must ignore unknown additive fields.
- breaking semantic changes require a new `specVersion`.
- publishers may require minimum consumer versions with `compatibility.minConsumerVersion`.
- a new message type, a new routing-key event type and a new job convention are additive: they
  keep `specVersion: '1.0'`.

How a 1.0 consumer treats a message type it does not know:

- **It ignores the message.** It acks it and drops it, and may log it; it does not reject it,
  dead-letter it or treat it as an error. `validateEnvelope` throws an
  `EdgeUnsupportedMessageTypeError` for a type it does not know, so a consumer can tell it from an
  invalid message. Builds of this library without that class throw a plain `Error` with the
  message `unsupported messageType: <type>`.
- **`node.hello` and `node.heartbeat` are the first such types.** A control plane that predates
  them receives them only if it binds the events exchange broadly (`edge.event.#`, or
  `edge.event.*.<siteId>.<nodeId>`); one that binds the execution event types by name never sees
  them. Either way it must ignore them as above.
- **Nodes never receive them.** Lifecycle messages flow from node to control plane only. A job
  consumer rejects, without requeue, anything on its queue that is not an `execution.job`.
- **Conventions travel inside `execution.job`.** A node that predates a convention sees an
  ordinary job with a capability it does not provide, and fails it with
  `UNSUPPORTED_CAPABILITY`. `node.hello` and the conventions arrived together, so a control plane
  should send a `package.install` job only to a node that has sent `node.hello`.

Recommended release policy:
- semver for package releases
- changelog entry for every contract-affecting change
- explicit migration notes for every breaking `specVersion` change

Runtime enforcement notes:
- compatibility checks are enforced on consumer inbound envelopes before handler execution
- compatibility checks are enforced on publisher outbound envelopes before publish
- a consumer configured without `runtimeConsumerVersion` rejects any job that sets `compatibility.minConsumerVersion`

