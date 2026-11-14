<div align="center">

# @vibeforge-systems/amqp-mcp-edge

**Typed AMQP execution channel for MCP edge workers**

[![Status: pre-1.0](https://img.shields.io/badge/status-pre--1.0-orange.svg)](#status-and-known-limitations)
[![CI](https://github.com/VibeForge-Systems/amqp-mcp-edge/actions/workflows/ci.yml/badge.svg)](https://github.com/VibeForge-Systems/amqp-mcp-edge/actions/workflows/ci.yml)
[![Node.js](https://img.shields.io/badge/Node.js-%E2%89%A522-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178c6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Versioned job and event contracts, AMQP consumer and publisher primitives, compatibility
validation, and an in-memory test double for the worker side of an MCP edge deployment.

</div>

---

## Overview

`@vibeforge-systems/amqp-mcp-edge` is a TypeScript library for **edge workers**: processes that
run close to the systems they act on, take execution jobs from a central MCP control plane over
RabbitMQ, and report acknowledgement, progress, results and failures back. It owns the broker
plumbing — envelopes, validation, routing, queue binding, backpressure — so worker code only has
to execute the job.

```mermaid
flowchart LR
  subgraph CONTROL["Control plane (your code)"]
    CP["Publishes execution.job<br/>Consumes lifecycle events"]
  end

  subgraph BROKER["RabbitMQ"]
    JOBS["mcp.edge.jobs"]
    EVENTS["mcp.edge.events"]
  end

  subgraph EDGE["Edge worker"]
    LIB["amqp-mcp-edge<br/>validate · route · ack/nack<br/>backpressure · reconnect"]
    WORKER["Your job handler"]
  end

  CP -->|execution.job| JOBS
  JOBS -->|delivery| LIB
  LIB --> WORKER
  WORKER --> LIB
  LIB -->|ack / progress / result / failure| EVENTS
  EVENTS --> CP
```

The library is **edge-side only**. A control plane publishes jobs with its own AMQP client; the
exported `buildJobRoutingKey` and the [wire protocol](docs/PROTOCOL.md) give it the routing keys
and envelope shapes to use.

### Relationship to amqp-mcp-transport

[amqp-mcp-transport](https://github.com/VibeForge-Systems/amqp-mcp-transport) carries **MCP
JSON-RPC sessions** over AMQP: a client and a server talking the Model Context Protocol through a
broker. This library carries **execution jobs**: fire-and-report work dispatched to workers, with
no JSON-RPC and no reply queues. The two use separate exchanges and routing namespaces and can
share a broker.

### What this library does

| Capability | Description |
|:---|:---|
| **Typed contracts** | Versioned envelopes for jobs, acks, progress, results and failures |
| **Runtime validation** | Inbound and outbound envelopes are validated at the boundary |
| **Compatibility checks** | Rejects jobs whose `minConsumerVersion` the worker does not meet |
| **Consumer primitive** | Job consumer that declares and binds its queue, or in passive mode only checks it, and re-subscribes after a connection drop |
| **Publisher primitive** | Event publisher that waits for channel drain under backpressure |
| **Node identity** | Every event carries the broker-validated AMQP `user_id` of the node, and `checkEventIdentity` checks it on the control-plane side |
| **Node lifecycle** | `node.hello` and `node.heartbeat` events with a capability snapshot (installed packages, runtimes, library version) and its digest |
| **Job conventions** | Typed deploy jobs (`package.install`: release, signed URL, expected dist hash) and tool jobs, with standard failure codes |
| **Least privilege** | Passive topology mode: a node runs without the configure permission |
| **Routing helpers** | Deterministic routing keys and queue names for edge execution traffic |
| **Health snapshots** | Structured connection and channel health for monitoring endpoints |
| **Testing helpers** | In-memory mock edge channel for fast, broker-free unit tests |

### What this library does _not_ do

- MCP transport or JSON-RPC semantics (see amqp-mcp-transport)
- Reply-queue driven execution flows
- Control-plane business logic, scheduling or job publishing
- Server-specific worker behavior

---

## Installation

> **Not yet published to npm.** The first release will be published from CI with
> [npm provenance](https://docs.npmjs.com/generating-provenance-statements). Until then, build
> from source (see [Development](#development)).

```bash
npm install @vibeforge-systems/amqp-mcp-edge
```

Requires **Node.js 22 or later**. The package is **ESM-only** and ships its own type declarations.

---

## Quick Start

```ts
import { createEdgeChannel } from '@vibeforge-systems/amqp-mcp-edge';

const channel = await createEdgeChannel({
  amqpUrl: 'amqp://localhost:5672',
  siteId: 'site-a',
  nodeId: 'node-1',
  // Jobs that set compatibility.minConsumerVersion are rejected unless this is set.
  runtimeConsumerVersion: '0.1.0',
  // Declares its own topology, which suits a local broker. In production, use
  // topologyMode: 'passive' (see Configuration).
});

channel.consumer.onJob(async (job, delivery) => {
  await channel.publisher.publishProgress({
    specVersion: '1.0',
    messageType: 'execution.progress',
    messageId: 'msg-progress-1',
    timestamp: new Date().toISOString(),
    producer: { service: 'edge-worker', version: '0.1.0' },
    compatibility: { channelVersion: '0.1.0', packageVersion: '0.1.0' },
    payload: {
      jobId: job.payload.jobId,
      attemptId: job.payload.attemptId,
      siteId: 'site-a',
      nodeId: 'node-1',
      state: 'running',
      percent: 25,
      message: 'Tool execution started',
    },
  });

  await channel.consumer.ack(delivery);
});

await channel.consumer.start();
```

---

## Configuration

`createEdgeChannel`, `EdgeJobConsumer` and `EdgeEventPublisher` take the same `EdgeChannelConfig`.

| Option | Default | Description |
|:---|:---|:---|
| `amqpUrl` | required | Broker URL (`amqp://` or `amqps://`) with the node's credentials and vhost |
| `topologyMode` | `'declare'` | `'declare'`: declare the exchanges, the consumer queue and its bindings, which needs the configure permission. `'passive'`: declare nothing; check that the consumer queue and the events exchange exist, and fail with an `EdgeTopologyError` naming any that is missing. **Use `'passive'` in production.** |
| `userId` | username in `amqpUrl` | Broker username sent as the AMQP `user_id` of every event; the broker refuses an event whose `user_id` is not the connection's user. Defaults to the URL-decoded username in `amqpUrl`, or `guest` (amqplib's default) when the URL has no credentials. Set it only if the username cannot be read from `amqpUrl` |
| `siteId` | — | Site segment of the queue name and routing keys |
| `nodeId` | — | Node segment of the queue name and routing keys |
| `nodeGroup` | — | Group whose jobs the node also takes |
| `jobsExchange` | `mcp.edge.jobs` | Exchange jobs arrive on |
| `eventsExchange` | `mcp.edge.events` | Exchange events are published to |
| `deadLetterExchange` | `mcp.edge.dlx` | The consumer queue's `x-dead-letter-exchange`, in declare mode. The library binds no queue to it; the platform declares the dead-letter queue |
| `consumerQueueName` | `mcp.edge.consumer.<siteId>.<nodeId>` | The queue the node consumes |
| `queueNamingStrategy` | built in | Computes the queue name when `consumerQueueName` is not set |
| `consumerQueueOptions` | `{ durable: true }` | Queue declaration options, in declare mode; ignored in passive mode |
| `consumerTag` | assigned by the broker | Consumer tag for the subscription |
| `prefetchCount` | `1` | Unacknowledged jobs the broker delivers at a time |
| `maxDeliveryAttempts` | `2` | Deliveries of a job whose handler throws, the first included. After the last, the consumer publishes `execution.failure` (`DELIVERY_ATTEMPTS_EXHAUSTED`, `retryable: false`) and dead-letters the job |
| `producer` | this library's name and version | The `producer` of events the library publishes itself, such as that failure |
| `maxMessageSizeBytes` | `1048576` | Larger jobs are rejected without requeue |
| `runtimeConsumerVersion` | — | Checked against a job's `compatibility.minConsumerVersion` |
| `runtimeProducerVersion` | — | Checked against an event's `compatibility.minProducerVersion` |
| `connectionName` | `amqp-mcp-edge` | Connection name shown by the broker |
| `reconnectEnabled` | `true` | Reconnect after the connection drops |
| `reconnectDelayMs` | `1000` | First reconnect delay; doubles after each failed attempt |
| `reconnectMaxDelayMs` | `30000` | Upper bound of the reconnect delay |
| `backpressureTimeoutMs` | `5000` | How long a publish waits for the channel to drain |
| `routingKeyStrategy` | — | Accepted, but not applied yet: events use the default routing keys |

### Production settings

```ts
const channel = await createEdgeChannel({
  // The node's own broker user; '@' in a username is written %40.
  amqpUrl: 'amqps://node-1%40site-a:<password>@broker.example.com/edge',
  siteId: 'site-a',
  nodeId: 'node-1',
  topologyMode: 'passive',
  runtimeConsumerVersion: '0.1.0',
  runtimeProducerVersion: '0.1.0',
});
```

- **`topologyMode: 'passive'`.** The control plane declares the exchanges, the node's queue and
  its bindings, and the dead-letter queue, as [docs/PROTOCOL.md](docs/PROTOCOL.md#broker-topology)
  lists. The node only checks, once at startup and once after each reconnect, that its queue and
  the events exchange exist. `createEdgeChannel` rejects with an `EdgeTopologyError` naming a
  missing one, closes its connection, and creates nothing.
- **One broker user per node, without configure.** The permission set is in
  [docs/PROTOCOL.md](docs/PROTOCOL.md#least-privilege-permissions): configure `^$`, read on the
  node's queue, write on `mcp.edge.events`, and topic write limited to the node's routing keys.
  The configure permission that declare mode needs would also let a node delete the shared
  exchanges every other node depends on.
- **The URL's username is the node's identity.** It is sent as `user_id` on every event, and the
  broker refuses an event whose `user_id` is not the authenticated user. Consumers of
  `mcp.edge.events` must reject events that lack it
  ([Node identity](docs/PROTOCOL.md#node-identity)).
- **The platform declares the dead-letter queue.** It binds a queue to `mcp.edge.dlx` before any
  node starts and watches its depth; the library never declares it, in either mode. Without it,
  rejected jobs are dropped ([Dead-letter queue](docs/PROTOCOL.md#dead-letter-queue)).
- **`amqps://`** for any broker not on the same host or a trusted private network.

`'declare'` remains the default in 0.x, for backward compatibility and local development against
a broker you own.

---

## Public Modules

Each module is available as a deep import:

| Import path | Purpose |
|:---|:---|
| `@vibeforge-systems/amqp-mcp-edge` | Top-level barrel — re-exports everything |
| `@vibeforge-systems/amqp-mcp-edge/config` | Channel config resolution, exchange/queue/routing defaults |
| `@vibeforge-systems/amqp-mcp-edge/contracts` | Envelope types, envelope validation, compatibility validation |
| `@vibeforge-systems/amqp-mcp-edge/consumer` | `EdgeJobConsumer` — queue binding, job dispatch, ack/nack |
| `@vibeforge-systems/amqp-mcp-edge/publisher` | `EdgeEventPublisher` — event publishing with backpressure |
| `@vibeforge-systems/amqp-mcp-edge/connection` | `createEdgeChannel` — managed AMQP connection lifecycle |
| `@vibeforge-systems/amqp-mcp-edge/health` | `createHealthSnapshot` — structured health reporting |
| `@vibeforge-systems/amqp-mcp-edge/testing` | `createMockEdgeChannel` — in-memory test double |

The message format itself is specified in [docs/PROTOCOL.md](docs/PROTOCOL.md).

---

## Usage Examples

### Contracts only (no broker)

Use the contract types and validation without connecting to a broker:

```ts
import type { ExecutionJobEnvelope } from '@vibeforge-systems/amqp-mcp-edge/contracts';
import { validateEnvelope } from '@vibeforge-systems/amqp-mcp-edge/contracts';

const job: ExecutionJobEnvelope = {
  specVersion: '1.0',
  messageType: 'execution.job',
  messageId: 'msg-job-1',
  timestamp: new Date().toISOString(),
  producer: { service: 'control-plane', version: '1.2.0' },
  compatibility: { channelVersion: '0.1.0', minConsumerVersion: '0.1.0' },
  payload: {
    jobId: 'job-1',
    attemptId: 'attempt-1',
    idempotencyKey: 'job-1-attempt-1',
    timeoutMs: 30000,
    target: { siteId: 'site-a', nodeId: 'node-1' },
    createdBy: { controlPlane: 'example-control-plane' },
    capability: { kind: 'tool', name: 'search.documents', version: '2026-03' },
    input: { query: 'recent jobs' },
  },
};

validateEnvelope(job); // throws an Error describing the first invalid field
```

### Announcing a node

A node sends `node.hello` after each connection and `node.heartbeat` on an interval; the rules are
in [Node Lifecycle Messages](docs/PROTOCOL.md#node-lifecycle-messages).

```ts
import { randomUUID } from 'node:crypto';
import { computeCapabilitiesDigest, createCapabilitySnapshot } from '@vibeforge-systems/amqp-mcp-edge';

const sessionId = randomUUID();
const startedAt = new Date().toISOString();
let sequence = 0;

const envelope = () => ({
  specVersion: '1.0' as const,
  messageId: randomUUID(),
  timestamp: new Date().toISOString(),
  producer: { service: 'edge-worker', version: '0.1.0' },
  compatibility: { channelVersion: '0.1.0' },
});

const capabilities = createCapabilitySnapshot({ packages: [] });
const node = { siteId: 'site-a', nodeId: 'node-1', sessionId, heartbeatIntervalMs: 30_000 };

await channel.publisher.publishNodeHello({
  ...envelope(),
  messageType: 'node.hello',
  payload: { ...node, sequence: ++sequence, startedAt, capabilities, capabilitiesDigest: computeCapabilitiesDigest(capabilities) },
});

setInterval(() => {
  void channel.publisher.publishNodeHeartbeat({
    ...envelope(),
    messageType: 'node.heartbeat',
    payload: { ...node, sequence: ++sequence, capabilitiesDigest: computeCapabilitiesDigest(capabilities) },
  });
}, node.heartbeatIntervalMs);
```

### Handling a deploy job

```ts
import { EdgeConventionError, isPackageInstallJob, parsePackageInstallJob } from '@vibeforge-systems/amqp-mcp-edge';

channel.consumer.onJob(async (job, delivery) => {
  if (isPackageInstallJob(job)) {
    try {
      const { release, download, distHash } = parsePackageInstallJob(job);
      // Download download.url (a credential: never log it), compute the dist hash over the archive's
      // contents, compare it with distHash as an exact string, and install only on a match.
    } catch (error) {
      if (error instanceof EdgeConventionError) {
        // Publish execution.failure with code INVALID_INPUT and retryable: false, then ack.
      }
    }
  }

  await channel.consumer.ack(delivery);
});
```

### Mock channel for tests

```ts
import { createMockEdgeChannel } from '@vibeforge-systems/amqp-mcp-edge/testing';

const mock = createMockEdgeChannel({ siteId: 'site-a', nodeId: 'node-1' });

mock.consumer.onJob(async (job, delivery) => {
  // your handler logic
  await mock.consumer.ack(delivery);
});

await mock.consumer.start();
await mock.dispatchJob(job);              // job: an ExecutionJobEnvelope, as above
console.log(mock.getPublishedEvents());   // inspect published events
```

---

## Status and known limitations

This is a **0.x** library: the envelope contract (`specVersion: '1.0'`) is stable, but the
runtime still has gaps that matter for production use. They will be closed before 1.0:

- **No publisher confirms.** Events are published on a plain channel, so a broker-side loss after
  `publish` returns is not detected.
- **RabbitMQ 4.x refuses transient non-exclusive queues.** The default consumer queue is durable
  and unaffected. If you pass `consumerQueueOptions` with `durable: false`, also set
  `exclusive: true`; otherwise the broker closes the connection on declare.
- **Reconnect is connection-level.** Channel-only closures are not handled, and deliveries
  received before a reconnect cannot be acknowledged afterwards (the broker redelivers them). In
  passive mode, a consumer whose queue disappeared while it was disconnected stops, and `health()`
  reports `consumerActive: false` with the reason in `details.topologyError`; it does not retry.

---

## Development

### Prerequisites

| Tool | Version |
|:---|:---|
| Node.js | >= 22 |
| npm | >= 10 |
| Docker | For integration tests (optional) |

### Getting started

```bash
git clone https://github.com/VibeForge-Systems/amqp-mcp-edge.git
cd amqp-mcp-edge
npm ci
npm run build
npm test
```

### Available scripts

| Command | Description |
|:---|:---|
| `npm run build` | Compile TypeScript to `dist/` |
| `npm test` | Run unit tests |
| `npm run test:unit` | Unit tests only |
| `npm run test:integration` | Integration tests against a live broker |
| `npm run test:all` | Unit + integration tests |
| `npm run release:check` | Full release gate (tests + pack dry-run) |
| `npm run example:rabbitmq` | End-to-end RabbitMQ demo |
| `npm run docker:rabbitmq:up` | Start local RabbitMQ container |
| `npm run docker:rabbitmq:down` | Stop local RabbitMQ container |

---

## RabbitMQ Integration

### Start a local broker

```bash
npm run docker:rabbitmq:up
```

The container creates the user `mcp` / `discovery`, so the default broker URL is
`amqp://mcp:discovery@127.0.0.1:5672`. The integration tests and the example try that URL and
then `amqp://guest:guest@127.0.0.1:5672`, unless you point them elsewhere.

### Run integration tests

```bash
npm run test:integration
```

Point the tests and the example at another broker with environment variables
(`.env.example` lists them):

```bash
# bash / zsh
export AMQP_INTEGRATION_URL=amqp://user:pass@broker.example.com:5672
export AMQP_EXAMPLE_URL=$AMQP_INTEGRATION_URL
```

```powershell
# PowerShell (or dot-source scripts/Use-LocalRabbitMQ.ps1 for the local defaults)
$env:AMQP_INTEGRATION_URL = "amqp://user:pass@broker.example.com:5672"
```

The integration suite covers:

- Full job lifecycle (dispatch → ack → progress → result)
- Failure + requeue behavior via `nack(..., { requeue: true })`
- A handler that always throws, on a classic and on a quorum queue: the job is delivered
  `maxDeliveryAttempts` times, reported as `DELIVERY_ATTEMPTS_EXHAUSTED`, and dead-lettered, and
  the job queued behind it runs
- Passive topology: a missing queue or events exchange fails startup with an `EdgeTopologyError`,
  creates nothing, and leaves no reconnect loop running
- Least privilege: a passive-mode worker whose user has configure `^$`, read on its own queue and
  write on `mcp.edge.events` runs jobs end to end, and its rejected jobs reach the dead-letter queue
- The same user's attempts to delete `mcp.edge.jobs` and `mcp.edge.events`, or to redeclare or
  rebind the topology, are refused with `ACCESS_REFUSED`
- Every event carries the node's `user_id`, and an event with a forged `user_id` closes the
  channel with `PRECONDITION_FAILED`
- After the broker drops the connection, a passive consumer whose queue has gone stops and
  reports why, with no reconnect loop, while the publisher keeps working

The least-privilege tests create a vhost and a broker user through the RabbitMQ management API,
then delete them. By default they use port 15672 on the broker host (the docker compose file
publishes it) with the credentials of the AMQP URL, which must belong to an administrator.
`AMQP_INTEGRATION_MANAGEMENT_URL`, `AMQP_INTEGRATION_MANAGEMENT_USERNAME` and
`AMQP_INTEGRATION_MANAGEMENT_PASSWORD` override them. Without the management API those tests are
skipped, with the reason; `AMQP_INTEGRATION_REQUIRE_MANAGEMENT=1` makes them fail instead, as CI
does.

### End-to-end example

Simulates the complete control-plane ↔ edge-worker flow over a real broker:

```bash
npm run example:rabbitmq
```

`examples/end-to-end-rabbitmq.mjs` plays both roles: a control plane publishing a job with plain
`amqplib`, and an edge worker consuming it through this library and reporting back.

---

## Project Structure

```
src/
├── config/          # Channel configuration and routing defaults
├── connection/      # Managed AMQP connection lifecycle
├── consumer/        # EdgeJobConsumer — queue binding and job dispatch
├── contracts/       # Envelope types, validation, compatibility logic
├── health/          # Structured health snapshots
├── publisher/       # EdgeEventPublisher — event publishing with backpressure
├── testing/         # In-memory mock edge channel
├── topology/        # Passive topology checks and EdgeTopologyError
└── __tests__/       # Unit and integration test suites
```

---

## Release

- [CHANGELOG.md](CHANGELOG.md) — release history.
- [docs/RELEASE_POLICY.md](docs/RELEASE_POLICY.md) — versioning rules and release gates.

```bash
npm run release:check
```

---

## Contributing and security

Issues and pull requests are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Report
vulnerabilities privately as described in [SECURITY.md](SECURITY.md), never in a public issue.
Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

[MIT](LICENSE) © VibeForge Systems, Inc.

---

<div align="center">

Built by [VibeForge Systems](https://www.vibeforgesystems.com/) · see also
[amqp-mcp-transport](https://github.com/VibeForge-Systems/amqp-mcp-transport)

</div>
