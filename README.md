<div align="center">

# @nagual69/amqp-mcp-edge

**Typed AMQP execution channel for MCP edge workers**

[![TypeScript](https://img.shields.io/badge/TypeScript-5.8-3178c6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-%E2%89%A518-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![npm](https://img.shields.io/badge/npm-%40nagual69%2Famqp--mcp--edge-cb3837?logo=npm)](https://www.npmjs.com/package/@nagual69/amqp-mcp-edge)

Versioned job and event contracts, AMQP consumer/publisher primitives, compatibility validation,
and in-memory testing helpers — everything an edge worker needs to talk to the MCP control plane.

</div>

---

## Overview

`@nagual69/amqp-mcp-edge` is a focused TypeScript library that provides the AMQP messaging layer for MCP edge worker deployments. It handles the broker communication so your worker code can focus on tool execution.

```mermaid
flowchart LR
  subgraph CONTROL["Control Plane"]
    CP["Dispatches execution.job<br/>Receives lifecycle events"]
  end

  subgraph LIBRARY["Library"]
    VALIDATION["Typed job envelopes<br/>Zod runtime validation"]
    FLOW["Ack / Progress / Result flow<br/>Compatibility enforcement"]
    RESILIENCE["Auto-reconnect<br/>Backpressure handling"]
  end

  subgraph BROKER["Broker"]
    RMQ["RabbitMQ<br/>Exchanges and queues"]
  end

  CP -->|producer path:<br>execution.job| VALIDATION
  VALIDATION --> FLOW
  FLOW -->|transport path:<br>publish/consume| RMQ
  RMQ -->|consumer path:<br>deliveries| RESILIENCE
  RESILIENCE -->|consumer path:<br>ack/progress/result/failure| CP

  classDef producer fill:#fff4d6,stroke:#b7791f,color:#5f370e,stroke-width:2px;
  classDef library fill:#e8f1ff,stroke:#2563eb,color:#0f172a,stroke-width:2px;
  classDef broker fill:#e8fff3,stroke:#0f766e,color:#134e4a,stroke-width:2px;

  class CP producer;
  class VALIDATION,FLOW,RESILIENCE library;
  class RMQ broker;

  linkStyle 0 stroke:#b7791f,stroke-width:3px;
  linkStyle 1 stroke:#2563eb,stroke-width:2px;
  linkStyle 2 stroke:#0f766e,stroke-width:3px;
  linkStyle 3 stroke:#0f766e,stroke-width:3px;
  linkStyle 4 stroke:#2563eb,stroke-width:3px;
```

### What this library does

| Capability | Description |
|:---|:---|
| **Typed contracts** | Versioned envelopes for jobs, acks, progress, results, and failures |
| **Runtime validation** | Zod schemas enforce envelope shape at the boundary |
| **Compatibility checks** | Rejects messages from incompatible producer/consumer versions |
| **Consumer primitive** | Job consumer with automatic queue binding and reconnect lifecycle |
| **Publisher primitive** | Event publisher with drain-wait backpressure handling |
| **Routing helpers** | Deterministic routing keys and queue names for edge execution traffic |
| **Health snapshots** | Structured connection/channel health for monitoring endpoints |
| **Testing helpers** | In-memory mock edge channel for fast, broker-free unit tests |

### What this library does _not_ do

- MCP Transport or JSON-RPC semantics
- Reply-queue driven execution flows
- Control-plane business logic
- Server-specific worker behavior

---

## Installation

```bash
npm install @nagual69/amqp-mcp-edge
```

> Requires **Node.js 18+** and **TypeScript 5.x** (ESM).

---

## Quick Start

```ts
import { createEdgeChannel } from '@nagual69/amqp-mcp-edge';

const channel = await createEdgeChannel({
  amqpUrl: 'amqp://localhost:5672',
  siteId: 'site-a',
  nodeId: 'node-1',
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

Each module is available as a deep import for tree-shaking and focused dependency graphs:

| Import path | Purpose |
|:---|:---|
| `@nagual69/amqp-mcp-edge` | Top-level barrel — re-exports everything |
| `@nagual69/amqp-mcp-edge/config` | Channel config resolution, exchange/queue/routing defaults |
| `@nagual69/amqp-mcp-edge/contracts` | Envelope types, Zod schemas, compatibility validation |
| `@nagual69/amqp-mcp-edge/consumer` | `EdgeJobConsumer` — queue binding, job dispatch, ack/nack |
| `@nagual69/amqp-mcp-edge/publisher` | `EdgeEventPublisher` — event publishing with backpressure |
| `@nagual69/amqp-mcp-edge/connection` | `createEdgeChannel` — managed AMQP connection lifecycle |
| `@nagual69/amqp-mcp-edge/health` | `createHealthSnapshot` — structured health reporting |
| `@nagual69/amqp-mcp-edge/testing` | `createMockEdgeChannel` — in-memory test double |

---

## Usage Examples

### Contracts only (no broker)

Use the contract types and validation without connecting to a broker:

```ts
import type { ExecutionJobEnvelope } from '@nagual69/amqp-mcp-edge/contracts';
import { validateEnvelope } from '@nagual69/amqp-mcp-edge/contracts';

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
    createdBy: { controlPlane: 'mcp-od-marketplace' },
    capability: { kind: 'tool', name: 'search.documents', version: '2026-03' },
    input: { query: 'recent jobs' },
  },
};

validateEnvelope(job); // throws ZodError on invalid shape
```

### Mock channel for tests

```ts
import { createMockEdgeChannel } from '@nagual69/amqp-mcp-edge/testing';

const mock = createMockEdgeChannel({ siteId: 'site-a', nodeId: 'node-1' });

mock.consumer.onJob(async (job, delivery) => {
  // your handler logic
  await mock.consumer.ack(delivery);
});

await mock.consumer.start();
await mock.simulateJob(jobEnvelope);      // dispatches to handler
console.log(mock.publishedEvents);        // inspect published events
```

---

## Development

### Prerequisites

| Tool | Version |
|:---|:---|
| Node.js | >= 18 |
| npm | >= 9 |
| Docker | For integration tests (optional) |

### Getting started

```bash
git clone https://github.com/VibeForge-Systems/amqp-mcp-edge.git
cd AMQPConnectorforMCPEdge
npm install
npm run build
npm test
```

### Environment setup

| File | Purpose |
|:---|:---|
| `.env.example` | Committed template with safe defaults |
| `.env.local` | Your local overrides (git-ignored) |

| Variable | Used by |
|:---|:---|
| `AMQP_INTEGRATION_URL` | Integration tests |
| `AMQP_EXAMPLE_URL` | End-to-end RabbitMQ example |

Copy the template and customize:

```bash
cp .env.example .env.local
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

Default broker URL: `amqp://mcp:discovery@127.0.0.1:5672`

### Run integration tests

```bash
npm run test:integration
```

Override the broker URL when needed:

```bash
# Windows
set AMQP_INTEGRATION_URL=amqp://mcp:discovery@127.0.0.1:5672

# PowerShell
$env:AMQP_INTEGRATION_URL = "amqp://mcp:discovery@127.0.0.1:5672"

# Or load the convenience helper
. .\scripts\Use-LocalRabbitMQ.ps1
```

The integration suite covers:

- Full job lifecycle (dispatch → ack → progress → result)
- Failure + requeue behavior via `nack(..., { requeue: true })`

### End-to-end example

Simulates the complete control-plane ↔ edge-worker flow over a real broker:

```bash
npm run example:rabbitmq
```

The example in `examples/end-to-end-rabbitmq.mjs` demonstrates a control-plane dispatching a job, an edge worker consuming it, and the round-trip event flow back to the control plane.

---

## Project Structure

```
src/
├── config/          # Channel configuration and routing defaults
├── connection/      # Managed AMQP connection lifecycle
├── consumer/        # EdgeJobConsumer — queue binding and job dispatch
├── contracts/       # Envelope types, Zod schemas, compatibility logic
├── health/          # Structured health snapshots
├── publisher/       # EdgeEventPublisher — event publishing with backpressure
├── testing/         # In-memory mock edge channel
└── __tests__/       # Unit and integration test suites
```

---

## Release

- See [CHANGELOG.md](CHANGELOG.md) for release history.
- See [docs/RELEASE_POLICY.md](docs/RELEASE_POLICY.md) for versioning rules and release gates.

Verify publish readiness:

```bash
npm run release:check
```

Published as a **public scoped package** with [npm provenance](https://docs.npmjs.com/generating-provenance-statements) enabled.

---

## License

[MIT](https://opensource.org/licenses/MIT) — see `package.json` for details.