# Release Policy

## Package Scope

`@vibeforge-systems/amqp-mcp-edge` is a reusable AMQP execution-channel library for MCP edge workers. It is versioned and released independently from any single MCP server or control plane.

## Versioning Rules

- Package releases use Semantic Versioning.
- `specVersion` changes only for breaking envelope-shape or envelope-semantics changes.
- Additive payload fields are minor-version compatible.
- Consumers must ignore unknown additive fields.
- Breaking semantic changes require both:
  - a new package major version when the public API changes
  - a new envelope `specVersion` when message compatibility changes

## Release Gates

Before publishing a release, verify all of the following:

1. `npm run build` passes.
2. `npm run test:unit` passes.
3. `npm run test:integration` passes against a real RabbitMQ broker.
4. README usage examples reflect the current public API.
5. `CHANGELOG.md` contains the release notes for the version being published.
6. Any contract-affecting change includes migration notes when needed.

## Changelog Policy

- Every contract-affecting change must be documented.
- Every public API addition, removal, or behavior change must be documented.
- Breaking changes must describe the old behavior, new behavior, and migration path.

## Tagging And Publish Flow

Recommended first-publish flow:

1. Update `package.json` version.
2. Move notable entries from `Unreleased` to a dated version section in `CHANGELOG.md`.
3. Run unit and integration tests.
4. Create a git tag matching the package version, for example `v0.1.0`.
5. Publish to the package registry.

## Compatibility Notes

- Public message contracts must remain transport-agnostic and must not adopt JSON-RPC or reply-queue semantics.
- Routing namespaces must remain separate from the [amqp-mcp-transport](https://github.com/VibeForge-Systems/amqp-mcp-transport) namespaces.
- Control-plane-specific behavior must stay outside this package.