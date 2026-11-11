# Contributing

Thanks for helping. Bug reports, fixes, tests and documentation are all welcome. Everyone taking
part follows the [Code of Conduct](CODE_OF_CONDUCT.md). Report security problems privately, as
[SECURITY.md](SECURITY.md) describes, never in a public issue.

## Set up

```bash
git clone https://github.com/VibeForge-Systems/amqp-mcp-edge.git
cd amqp-mcp-edge
npm ci
```

Node 22 or later. The unit tests need no broker. The integration tests and the end-to-end example
need RabbitMQ: `npm run docker:rabbitmq:up` starts one, as the [README](README.md#rabbitmq-integration)
shows.

## Before you open a pull request

Run what CI runs:

```bash
npm run typecheck
npm test
npm run test:integration   # with a broker running
npm pack --dry-run
```

## What a change needs

- **A test.** A bug fix comes with a test that fails without it.
- **Wire-format changes come with the protocol.** [docs/PROTOCOL.md](docs/PROTOCOL.md) describes
  what goes on the broker. A change to an envelope, a routing key or a compatibility rule updates
  it in the same pull request, and follows the versioning rules in
  [docs/RELEASE_POLICY.md](docs/RELEASE_POLICY.md).
- **A CHANGELOG entry** under `[Unreleased]`, for anything a user of the package would notice.
- **Strict TypeScript.** No new runtime dependencies without discussion in an issue first.

## Pull requests

Branch from `main` and keep each pull request to one change. Say what it changes, why, and how you
tested it. By contributing, you agree that your contribution is licensed under the
[MIT License](LICENSE).
