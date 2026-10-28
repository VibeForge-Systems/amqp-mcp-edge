## Plan: AMQPConnectorforMCPEdge Reusable MCP Edge Library

Create `AMQPConnectorforMCPEdge` as a first-class reusable MCP edge library with its own versioned ownership and independent release cadence. The recommended approach is to make this repo the generic AMQP execution-channel package for edge-style MCP deployments, analogous to how `AMQPConnectorforMCP` serves as a generic MCP transport package. This library should be reusable by `mcp_open_discovery_3_0` and by future MCP servers that need outbound-only edge workers, control-plane job dispatch, progress events, and result publication.

**Current Status Snapshot**

- Overall status: Mostly completed.
- Implemented and validated: package scope, public modules, worker-oriented runtime primitives, routing and namespace separation, envelope validation, compatibility enforcement, reconnect handling, backpressure handling, mock-broker tests, live RabbitMQ integration tests, release policy, changelog, and package identity alignment.
- Remaining partial items: explicit credential-redaction utility/practice and an external cross-repo compatibility slice with real marketplace and edge runtime repositories.
- Evidence sources: `docs/TRACEABILITY_MATRIX.md`, `docs/REMEDIATION_PLAN.md`, `README.md`, `src/__tests__/edge-library.test.ts`, and `src/__tests__/rabbitmq.integration.test.ts`.

**Steps**
1. Phase 1: Freeze the repo role. This repo is a reusable MCP edge library for AMQP-based execution channels. It is not application business logic and it is not a server-specific implementation detail. Status: Completed.
2. Phase 1: Define the library contract in generic terms so it can serve multiple MCP servers, not just Open Discovery. The contract should include `ExecutionJobEnvelope`, `JobAck`, `JobProgressEvent`, `JobResultEvent`, `JobFailureEvent`, attempt metadata, idempotency metadata, target identity, capability/version headers, and compatibility negotiation fields. Status: Completed.
3. Phase 1: Define explicit non-goals. Do not implement the MCP `Transport` interface here, do not model the protocol as JSON-RPC request/response, and do not rely on `replyTo` plus `correlationId` as the primary execution lifecycle pattern. Status: Completed.
4. Phase 1: Define the packaging goal clearly: publish and version this library as the shared dependency that multiple edge-capable MCP runtimes can consume. Status: Completed.
5. Phase 1: Decide whether the library exposes direct `amqplib` bindings or a thin adapter abstraction so consuming runtimes can swap AMQP client implementations later without changing the public job/event contract. Status: Completed for the current release as direct `amqplib` bindings; adapter abstraction remains a future design option, not an implemented goal.
6. Phase 2: Pull forward only the beneficial reusable foundations from `AMQPConnectorforMCP`: connection lifecycle management, reconnect/channel recovery patterns, safe ack/nack handling, validation utilities, config validation, routing-key strategy patterns, max-message-size checks, credential-redaction practices, and mocked broker testing patterns. Status: Mostly completed; credential-redaction practices remain partial.
7. Phase 2: Define exchange, queue, routing-key, and namespace conventions specifically for reusable edge execution so job traffic can never be confused with MCP transport traffic. Status: Completed.
8. Phase 2: Provide library primitives aligned to worker semantics rather than transport semantics: consumer startup, job receipt, ack/nack, retry signaling, progress emission, result emission, terminal failure emission, health reporting, and backpressure hooks. Status: Completed.
9. Phase 2: Add schema validation and versioning for every public envelope and metadata block so the control plane and any consuming edge runtime can evolve independently within clear compatibility rules. Status: Completed.
10. Phase 2: Add documentation and examples for multiple-consumer scenarios: Open Discovery as one consumer, future MCP edge runtimes as additional consumers, and control-plane publishers as separate producers. Status: Mostly completed; generic documentation and examples exist, but no external second-runtime compatibility slice has been executed.
11. Phase 3: Build one compatibility slice with `mcp-od-marketplace` and `mcp_open_discovery_3_0`: publish one generic job envelope, consume it through the library, emit progress, emit final result, and verify schema-version compatibility end to end. Status: Partial; simulated by generic integration tests, but not yet proven via an external cross-repo contract test.
12. Phase 3: Publish docs that make the repo boundary unambiguous: this is the reusable MCP edge execution library for AMQP, while `AMQPConnectorforMCP` remains the reusable MCP transport library for local/direct client-server transport. Status: Completed.
13. Phase 4: Establish release ownership: semantic versioning, compatibility guarantees, changelog policy, and a clear deprecation path so downstream MCP servers can adopt the library safely. Status: Mostly completed; release policy, changelog, and semver guidance are in place, while future deprecation-path detail can be expanded when needed.

**Concrete Package APIs**

The first public surface should be small, explicit, and worker-oriented.

Recommended package modules:
- `@nagual69/amqp-mcp-edge/config`
- `@nagual69/amqp-mcp-edge/contracts`
- `@nagual69/amqp-mcp-edge/publisher`
- `@nagual69/amqp-mcp-edge/consumer`
- `@nagual69/amqp-mcp-edge/health`
- `@nagual69/amqp-mcp-edge/testing`

Recommended exported types:
- `EdgeChannelConfig`
- `ExecutionJobEnvelope`
- `JobAckMessage`
- `JobProgressEvent`
- `JobResultEvent`
- `JobFailureEvent`
- `CompatibilityMetadata`
- `TargetSelector`
- `ExecutionAttemptMetadata`
- `MessageEnvelopeVersion`
- `DeliveryHeaders`

Recommended exported classes and factories:
- `EdgeJobConsumer`
- `EdgeEventPublisher`
- `EdgeChannelConnectionManager`
- `createEdgeChannel(config)`
- `validateEnvelope(payload)`
- `validateConfig(config)`
- `buildRoutingKey(messageType, selector)`
- `createMockEdgeChannel()`

Recommended consumer-facing interface:

```ts
export interface EdgeJobConsumer {
	start(): Promise<void>;
	stop(): Promise<void>;
	onJob(handler: (job: ExecutionJobEnvelope, context: JobDeliveryContext) => Promise<void>): void;
	ack(delivery: JobDeliveryContext): Promise<void>;
	nack(delivery: JobDeliveryContext, options?: { requeue?: boolean; reason?: string }): Promise<void>;
	health(): Promise<EdgeChannelHealth>;
}
```

Recommended publisher-facing interface:

```ts
export interface EdgeEventPublisher {
	publishAck(message: JobAckMessage): Promise<void>;
	publishProgress(message: JobProgressEvent): Promise<void>;
	publishResult(message: JobResultEvent): Promise<void>;
	publishFailure(message: JobFailureEvent): Promise<void>;
	health(): Promise<EdgeChannelHealth>;
}
```

Recommended high-level composition API:

```ts
export interface EdgeChannel {
	consumer: EdgeJobConsumer;
	publisher: EdgeEventPublisher;
	close(): Promise<void>;
}

export async function createEdgeChannel(config: EdgeChannelConfig): Promise<EdgeChannel>;
```

**Message Contracts**

All public messages should use an explicit typed envelope rather than raw broker payloads or JSON-RPC semantics.

Base envelope shape:

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

Compatibility block:

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

Execution job payload:

```ts
export interface ExecutionJobPayload {
	jobId: string;
	attemptId: string;
	idempotencyKey: string;
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

Ack payload:

```ts
export interface JobAckPayload {
	jobId: string;
	attemptId: string;
	nodeId: string;
	accepted: boolean;
	reason?: string;
}

export type JobAckMessage = BaseEdgeEnvelope<'execution.ack', JobAckPayload>;
```

Progress payload:

```ts
export interface JobProgressPayload {
	jobId: string;
	attemptId: string;
	nodeId: string;
	state: 'received' | 'started' | 'running' | 'waiting' | 'finishing';
	percent?: number;
	message?: string;
	metrics?: Record<string, number>;
}

export type JobProgressEvent = BaseEdgeEnvelope<'execution.progress', JobProgressPayload>;
```

Result payload:

```ts
export interface JobResultPayload {
	jobId: string;
	attemptId: string;
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

Failure payload:

```ts
export interface JobFailurePayload {
	jobId: string;
	attemptId: string;
	nodeId: string;
	failedAt: string;
	retryable: boolean;
	code: string;
	message: string;
	details?: Record<string, unknown>;
}

export type JobFailureEvent = BaseEdgeEnvelope<'execution.failure', JobFailurePayload>;
```

**Routing And Namespace Contract**

Status: Completed.

The library should standardize routing separately from MCP transport routing.

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

Recommended queue ownership:
- control plane owns job publication
- edge runtimes own job consumer queues
- control plane owns event consumer queues
- message contracts must never depend on ephemeral reply queues

**Versioning Rules**

Status: Completed.

The library should define simple, explicit compatibility rules:
- `specVersion` changes only for breaking envelope changes
- additive payload fields are minor-version compatible
- field removals or semantic meaning changes require a new `specVersion`
- consumers must ignore unknown additive fields
- publishers may require minimum consumer versions via `compatibility.minConsumerVersion`

**Testing Contract**

Status: Completed.

The initial test surface should verify:
1. envelope validation for all public message types
2. routing-key generation and namespace separation
3. ack, progress, result, and failure publication
4. consumer ack/nack semantics
5. version compatibility checks
6. mock-broker round trips without importing MCP transport code

**Relevant starting sources**
- `https://github.com/VibeForge-Systems/amqp-mcp-transport/tree/main/src/transports/amqp-client-transport.ts` - reuse connection lifecycle and client-side recovery ideas, not MCP message semantics.
- `https://github.com/VibeForge-Systems/amqp-mcp-transport/tree/main/src/transports/amqp-server-transport.ts` - reuse broker/channel management patterns and validation discipline, not request/response routing behavior.
- `https://github.com/VibeForge-Systems/amqp-mcp-transport/tree/main/src/transports/amqp-utils.ts` - strong starting point for shared validation, routing helpers, config parsing, and message size checks.
- `https://github.com/VibeForge-Systems/amqp-mcp-transport/tree/main/src/transports/__tests__/transport.test.ts` - strong starting point for mocked broker tests and flow verification.
- `https://github.com/VibeForge-Systems/amqp-mcp-transport/tree/main/AUDIT.md` - preserve the lessons already learned about transport contract clarity, recovery, validation, and application-logic separation.

**Verification**

Overall verification status: Mostly completed.

1. Verify this repo can be explained as a reusable MCP edge library without describing it as an MCP `Transport` implementation. Status: Completed.
2. Verify job envelopes, progress events, and result events are versioned, validated, and independent from JSON-RPC response correlation. Status: Completed.
3. Verify exchange and queue namespaces cannot collide with MCP-over-AMQP transport namespaces from `AMQPConnectorforMCP`. Status: Completed.
4. Verify the library can be consumed by `mcp_open_discovery_3_0` and by at least one hypothetical second MCP server without server-specific protocol leakage. Status: Partial; generic reuse and protocol isolation are implemented, but no external second-repo contract verification has been executed.
5. Verify the package can later support alternate boundary adapters or implementations without rewriting control-plane business logic. Status: Completed at the contract boundary level; runtime currently uses direct `amqplib` bindings.
6. Verify semantic versioning and release notes can communicate protocol changes cleanly to downstream runtimes. Status: Completed.

**Decisions**

Status: Completed and reflected in the current implementation.

- Include: reusable edge execution envelopes, producer/consumer helpers, validation, compatibility/versioning, worker-oriented delivery semantics, and generic packaging for multiple MCP servers.
- Exclude: direct MCP client/server transport implementation and control-plane business rules.
- Recommendation: treat this repo as the long-lived reusable AMQP edge library, parallel in purpose to `AMQPConnectorforMCP` but focused on edge execution channels instead of MCP transport semantics.