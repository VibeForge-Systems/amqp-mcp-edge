# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
follows [Semantic Versioning](https://semver.org/). Version 0.1.0 has not been published yet.

## [Unreleased]

### Added

- first package scaffold for the reusable AMQP MCP edge library
- versioned execution job and event contracts with runtime validation
- routing-key and queue naming helpers for edge execution traffic
- AMQP connection manager, job consumer, and event publisher primitives
- in-memory mock edge channel for tests and contract walkthroughs
- RabbitMQ-backed integration test workflow and live end-to-end example
- initial release policy and first-publish guidance
- `topologyMode` config option. `'passive'` never declares an exchange or queue and never binds:
  the consumer checks its queue and the publisher checks the events exchange with passive
  declares, once at startup and once after each reconnect, so a node user needs no configure
  permission. A missing entity fails startup with an `EdgeTopologyError` naming it, and
  `createEdgeChannel` then closes its connection. `'declare'` stays the default.
- `consumer.verifyTopology()` and `publisher.verifyTopology()`, the passive checks on their own
- every event now carries the AMQP `user_id` property, set to the node's broker username: the new
  `userId` config option, else the URL-decoded username in `amqpUrl`, else `guest`. The broker
  refuses a `user_id` that is not the connection's user. `resolveUserId` exports the derivation,
  and the mock channel records it on each published event
- `docs/PROTOCOL.md` documents the topology the control plane declares (exchanges, node queue,
  bindings, dead-letter queue), the least-privilege permission set, and `user_id` as the node
  identity
- integration tests that run a passive-mode worker as a least-privilege user, check that it cannot
  delete the shared exchanges, and check that a forged `user_id` is refused; they create the user
  through the management API, which CI now exposes on port 15672
- **Contract:** node lifecycle messages `node.hello` and `node.heartbeat`, published to
  `mcp.edge.events` as `edge.event.hello.<siteId>.<nodeId>` and
  `edge.event.heartbeat.<siteId>.<nodeId>`, which the existing node topic permission already
  covers. Both carry a session ID, a sequence number and the heartbeat interval; `node.hello`
  carries a capability snapshot (installed packages with their dist hashes, runtimes, this
  library's version), and both carry its digest, SHA-256 over the RFC 8785 canonical JSON. The
  validators reject a message whose digest does not match the snapshot it carries.
  `publisher.publishNodeHello` and `publisher.publishNodeHeartbeat` publish them, and the mock
  channel records them
- `createCapabilitySnapshot`, `computeCapabilitiesDigest` and `canonicalJson`, and
  `edgeLibraryName` and `edgeLibraryVersion`
- **Contract:** the deploy-job convention, `capability: { kind: 'task', name: 'package.install' }`
  with the release identity, a signed `https` download URL and the expected dist hash in `input`;
  `isPackageInstallJob`, `parsePackageInstallJob` and `packageInstallResultOutputSchema`
- **Contract:** the tool-job convention, `capability: { kind: 'tool', name }` with the MCP tool
  name, the tool's arguments as `input`, and an MCP `CallToolResult` as the result output;
  `isToolJob`, `parseToolJob` and `parseToolResultOutput`. Both conventions throw an
  `EdgeConventionError` listing every problem
- `edgeFailureCodes`, the standard `execution.failure` codes, and the reserved `package.*` and
  `node.*` task names
- `checkEventIdentity`, which applies the node-identity rules to a delivered event for a control
  plane, and `parseEventRoutingKey`, `buildNodeKey`, `sanitizeRoutingSegment` and
  `eventRoutingTypes`
- `EdgeUnsupportedMessageTypeError`, thrown by `validateEnvelope` for a `messageType` it does not
  know, so a consumer can ignore such a message, as the compatibility rules now require, instead
  of rejecting it
- `docs/PROTOCOL.md` specifies the lifecycle messages and their sending and liveness rules, the
  capability digest with a test vector, the job conventions, the failure codes, and how a 1.0
  consumer treats a message type it does not know

### Changed

- `zod` is now `^4.6.7` (was `^3.25.64`), still a regular dependency. The range admits no 4.x
  release before 4.5, the first to reject minute-precision timestamps
- **Contract:** the timestamp fields (`timestamp`, `payload.attempt.firstAttemptAt`,
  `payload.attempt.previousAttemptAt`, `payload.completedAt` and `payload.failedAt`) must be an
  RFC 3339 date-time with seconds and a `Z` or `±hh:mm` offset. Minute precision
  (`2026-11-12T10:00Z`) and an offset without the colon (`+0530`), which zod 3 accepted, are now
  rejected: the consumer rejects a job carrying one without requeue, and the publisher refuses
  an event carrying one.
  `new Date().toISOString()` output is unaffected. `docs/PROTOCOL.md` documents the format, and
  unit tests pin it
- **Contract:** the integer fields (`payload.timeoutMs`, `payload.attempt.attemptNumber` and
  `payload.attempt.maxAttempts`) must be safe integers, and progress `metrics` values must be
  finite numbers. zod 3 accepted integers above `Number.MAX_SAFE_INTEGER` and `Infinity`. JSON
  cannot carry `Infinity`, so that half affects only events built in process
- `EdgeEnvelope`, which `validateEnvelope` returns, also includes the two lifecycle messages.
  TypeScript code that reads `payload.jobId` from it must first narrow on `messageType`
- validation errors from `validateEnvelope` and `assertCompatibleEnvelope` keep their
  `invalid <messageType> envelope:` prefix, but the issue text after it is now zod 4's (for
  example `invalid_format` and `Invalid ISO datetime` where zod 3 gave `invalid_string` and
  `Invalid datetime`). Match on the prefix, not on the issue text
- `amqplib` is now `^2.3.1` (was `^0.10.8`). It fixes GHSA-37f9-7m79-6866, in which a broker
  could exhaust the client's memory, and so also caps incoming frames at 64 MiB by default
  (`maxFrameSize`, settable as an `amqpUrl` query parameter). amqplib now bundles its type
  definitions, so `@types/amqplib` is no longer a dev dependency
- `?heartbeat=0` in `amqpUrl` now disables heartbeats. With amqplib 0.10 it meant no preference,
  and the broker's suggested interval applied; omit the parameter to keep that behavior
- **Contract:** consumers of `mcp.edge.events` must reject events without `user_id`. Events from
  earlier builds of this library carry none, so upgrade edge nodes before enforcing the rule
- `createEdgeChannel` validates the config before it connects, so an invalid config no longer
  leaves an open connection behind
- the consumer's health snapshot reports a failed passive check in `details.topologyError`
- after a reconnect, every reconnect handler runs even if an earlier one fails
- the integration tests delete the exchanges they declare
- package published under the VibeForge Systems npm scope, as `@vibeforge-systems/amqp-mcp-edge`
- Node.js 22 or later is required (`engines.node`)
- the wire protocol is documented in `docs/PROTOCOL.md`; internal planning documents were removed
- the integration tests and the end-to-end example declare exclusive queues, so they run on
  RabbitMQ 4.x, which refuses transient non-exclusive queues; a failed test setup now closes its
  broker connection instead of leaving the test process running
- local docker compose and CI use RabbitMQ 4
- production build now excludes test compilation outputs from published `dist`
- added reconnect and channel re-initialization workflow in connection lifecycle
- runtime compatibility checks are now enforced by consumer and publisher paths
- publisher now waits for channel drain when AMQP backpressure is signaled
- **Contract:** the platform, not the library, declares the dead-letter queue and binds it to
  `mcp.edge.dlx`, in either topology mode; the library never declares a queue on it.
  `docs/PROTOCOL.md` documents what an operator declares, which jobs are dead-lettered, and why
  the queue must exist before any node starts

## [0.1.0] - unreleased

### Added

- initial publish candidate for amqp-mcp-edge
- public modules for config, contracts, consumer, publisher, connection, health, and testing
- unit tests covering envelope validation, compatibility validation, routing, and mock round trips