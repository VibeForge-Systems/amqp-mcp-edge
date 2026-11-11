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
| **Consumer primitive** | Job consumer with automatic queue binding and re-initialization after a connection drop |
| **Publisher primitive** | Event publisher that waits for channel drain under backpressure |
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
- **A throwing job handler requeues indefinitely.** There is no retry limit or poison-message
  handling yet; handle errors inside your handler and `nack` with `requeue: false` for permanent
  failures.
- **Dead-letter exchange is declared but not bound.** Jobs rejected with `requeue: false` are
  dropped unless you bind your own queue to `mcp.edge.dlx`.
- **RabbitMQ 4.x refuses transient non-exclusive queues.** The default consumer queue is durable
  and unaffected. If you pass `consumerQueueOptions` with `durable: false`, also set
  `exclusive: true`; otherwise the broker closes the connection on declare.
- **Reconnect is connection-level.** Channel-only closures are not handled, and deliveries
  received before a reconnect cannot be acknowledged afterwards (the broker redelivers them).

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
