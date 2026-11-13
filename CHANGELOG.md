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

### Changed

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

## [0.1.0] - unreleased

### Added

- initial publish candidate for amqp-mcp-edge
- public modules for config, contracts, consumer, publisher, connection, health, and testing
- unit tests covering envelope validation, compatibility validation, routing, and mock round trips