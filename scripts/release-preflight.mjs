#!/usr/bin/env node
// Checks what a release tag would publish, and writes its release notes.
//
//   node scripts/release-preflight.mjs <tag> [--notes <file>] [--allow-undated]
//
// It checks that:
// - the tag is vX.Y.Z or vX.Y.Z-<prerelease>;
// - the tag is v + package.json's version;
// - CHANGELOG.md has a non-empty "## [X.Y.Z] - YYYY-MM-DD" section.
//
// The release workflow runs it on the pushed tag. CI runs it with --allow-undated on
// v<package.json version>, so a PR that bumps the version must bring its CHANGELOG section too.
//
// On success it prints key=value lines for $GITHUB_OUTPUT:
//   version, tarball, prerelease, dist_tag
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));

function fail(message) {
  console.error(`::error::${message}`);
  process.exit(1);
}

let tag;
let notesFile;
let allowUndated = false;
const args = process.argv.slice(2);

for (let index = 0; index < args.length; index += 1) {
  if (args[index] === '--notes') {
    notesFile = args[++index];
  } else if (args[index] === '--allow-undated') {
    allowUndated = true;
  } else if (tag === undefined && !args[index].startsWith('--')) {
    tag = args[index];
  } else {
    fail(`unexpected argument ${args[index]}`);
  }
}

if (!tag) {
  fail('usage: release-preflight.mjs <tag> [--notes <file>] [--allow-undated]');
}

const match = /^v(\d+\.\d+\.\d+(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?)$/.exec(tag);
if (!match) {
  fail(`tag ${tag} is not vX.Y.Z or vX.Y.Z-<prerelease>`);
}

const [, version, prerelease] = match;
const pkg = JSON.parse(readFileSync(`${root}package.json`, 'utf8'));

if (pkg.version !== version) {
  fail(`tag ${tag} does not match package.json version ${pkg.version}`);
}

// The section runs from its heading to the next "## [" heading.
const changelog = readFileSync(`${root}CHANGELOG.md`, 'utf8').split('\n');
const escaped = version.replace(/[.+-]/g, '\\$&');
const heading = new RegExp(`^## \\[${escaped}\\](?:\\s+-\\s+(.*))?\\s*$`);
const start = changelog.findIndex((line) => heading.test(line));

if (start < 0) {
  fail(`CHANGELOG.md has no "## [${version}]" section`);
}

const date = heading.exec(changelog[start])[1];
if (!allowUndated && !/^\d{4}-\d{2}-\d{2}$/.test(date ?? '')) {
  fail(`the CHANGELOG.md heading for ${version} must carry the release date, as "## [${version}] - YYYY-MM-DD"`);
}

const end = changelog.findIndex((line, index) => index > start && line.startsWith('## ['));
const notes = changelog.slice(start + 1, end < 0 ? undefined : end).join('\n').trim();

if (notes === '') {
  fail(`the CHANGELOG.md section for ${version} is empty`);
}

if (notesFile) {
  writeFileSync(notesFile, `${notes}\n`);
}

// npm pack names a scoped package's tarball <scope>-<name>-<version>.tgz.
const tarball = `${pkg.name.replace(/^@/, '').replace('/', '-')}-${version}.tgz`;

console.log(`version=${version}`);
console.log(`tarball=${tarball}`);
console.log(`prerelease=${prerelease ? 'true' : 'false'}`);
// npm refuses to publish a prerelease as latest without an explicit tag.
console.log(`dist_tag=${prerelease ? 'next' : 'latest'}`);
