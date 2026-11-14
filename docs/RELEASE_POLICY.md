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

A release is a version tag. Pushing `vX.Y.Z` (or `vX.Y.Z-<prerelease>`) runs
[`.github/workflows/release.yml`](../.github/workflows/release.yml), which:

1. **checks the tag** with `scripts/release-preflight.mjs`: it must be `v` plus the
   `package.json` version, and `CHANGELOG.md` must have a non-empty `## [X.Y.Z] - YYYY-MM-DD`
   section. CI runs the same check, without the date, on every pull request;
2. **builds and tests**: `npm ci` with no dependency cache, typecheck, unit tests, integration
   tests against RabbitMQ 4 with the management API, build, and `npm pack`. This job can only read
   the repository;
3. **creates the GitHub Release** for the tag, with the release notes from the CHANGELOG section,
   the tarball `vibeforge-systems-amqp-mcp-edge-X.Y.Z.tgz` and its `.sha256`. A version with a
   prerelease part is marked as a prerelease. This is the only job that can write to the
   repository;
4. **publishes the same tarball to npm**, if publishing is turned on. It downloads the tarball from
   the release, checks its SHA-256 and that it holds this package at this version, and runs
   `npm publish <tarball> --provenance --access public`. A prerelease goes to the `next`
   dist-tag; any other version goes to `latest`. A version already on npm is left alone.

Steps for the maintainer:

1. Update the `package.json` version, and move the notable entries from `Unreleased` to a
   `## [X.Y.Z] - YYYY-MM-DD` section of `CHANGELOG.md`, in a pull request.
2. After it merges, tag the merge commit and push the tag:
   `git tag -a vX.Y.Z -m vX.Y.Z <commit> && git push origin vX.Y.Z`.
3. Check the run, the GitHub Release and, once published, the package page on npmjs.com, which
   shows the provenance. `npm audit signatures` verifies it from a project that installs the
   package.

### Turning on npm publishing

Publishing to npm is off until the repository variable `NPM_PUBLISH` is set. Until then the npm
job is skipped, and the run summary says so. An npm version cannot be taken back, so first
create the GitHub environment `npm` (Settings > Environments) and add a required reviewer to it:
the npm job runs in that environment, so every publish then waits for approval.

Choose one mode:

- **`NPM_PUBLISH=token`**. Store an npm granular access token as the secret `NPM_TOKEN` of the
  `npm` environment. The token needs read and write access to the `@vibeforge-systems` scope (or to
  this package), and it must bypass two-factor authentication, because a workflow cannot answer
  a 2FA prompt. The job fails
  if the secret is missing. Provenance is signed with the job's OIDC token. The first release
  needs this mode: npm configures trusted publishing in a package's settings, so the package must
  exist first.
- **`NPM_PUBLISH=trusted`** (preferred once the package exists). This mode stores no secret. In
  the package's settings on npmjs.com (Settings > Trusted publishing), add a GitHub Actions
  trusted publisher with:
  - organization `VibeForge-Systems`;
  - repository `amqp-mcp-edge`;
  - workflow filename `release.yml`;
  - environment `npm`;
  - `npm publish` allowed, not only `npm stage publish`.

  npm expires a new trusted publisher that has not published within 2 days, so add it shortly
  before the next release. The job installs npm 11.21.1, because trusted publishing needs npm
  11.5.1 or later. npm exchanges the job's OIDC token for a short-lived publish token, and
  generates provenance itself, because the repository and the package are public. Once trusted
  publishing works, revoke the token and delete `NPM_TOKEN`.

To publish a release that was tagged while publishing was off, run the workflow by hand: Actions
> Release > Run workflow, with the tag (for example `v0.1.0`). That run builds nothing. It
publishes the tarball already attached to the GitHub Release.

## Compatibility Notes

- Public message contracts must remain transport-agnostic and must not adopt JSON-RPC or reply-queue semantics.
- Routing namespaces must remain separate from the [amqp-mcp-transport](https://github.com/VibeForge-Systems/amqp-mcp-transport) namespaces.
- Control-plane-specific behavior must stay outside this package.