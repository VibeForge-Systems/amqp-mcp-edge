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

### Changed

- package published under the VibeForge Systems npm scope, as `@vibeforge-systems/amqp-mcp-edge`
- Node.js 22 or later is required (`engines.node`)
- the wire protocol is documented in `docs/PROTOCOL.md`; internal planning documents were removed
- production build now excludes test compilation outputs from published `dist`
- added reconnect and channel re-initialization workflow in connection lifecycle
- runtime compatibility checks are now enforced by consumer and publisher paths
- publisher now waits for channel drain when AMQP backpressure is signaled

## [0.1.0] - unreleased

### Added

- initial publish candidate for amqp-mcp-edge
- public modules for config, contracts, consumer, publisher, connection, health, and testing
- unit tests covering envelope validation, compatibility validation, routing, and mock round trips