# Releasing Cowrie

How to cut a release and publish Cowrie to every registry. Cowrie is a single
codec shipped as four packages from one monorepo:

| Language | Package        | Registry  | Source subdir |
|----------|----------------|-----------|---------------|
| Python   | `cowrie-ref`   | PyPI      | `tools/`      |
| TS / JS  | `cowrie-codec` | npm       | `typescript/` |
| Rust     | `cowrie-rs`    | crates.io | `rust/`       |
| Go       | `github.com/Neumenon/cowrie/go` | none (VCS tag) | `go/` |

All four implementations are currently at **0.9.0** (pre-1.0 — the wire format
is a release candidate and is **not yet frozen**; see [Freezing to 1.0](#freezing-to-10)).

The tag drives everything. Pushing a release tag `vX.Y.Z` triggers
`.github/workflows/publish.yml`, which runs the full conformance gate, verifies
that all manifests already declare `X.Y.Z`, then publishes each package. **A tag
never ships red** — every publish job depends on the gate passing.

---

## One-time registry setup

Do this once, before the first publish. None of it lives in the repo; it is all
account/registry configuration.

- **PyPI — Trusted Publisher (OIDC), no token.** On the `cowrie-ref` project
  settings (`https://pypi.org/manage/project/cowrie-ref/settings/publishing/`),
  add a Trusted Publisher for this repo's `publish.yml` workflow, environment
  `pypi`. The `pypi` job authenticates via OIDC — no API token is stored.
- **npm — secret `NPM_TOKEN`.** Create an npm automation token with publish
  rights to `cowrie-codec`, and add it as the repo secret `NPM_TOKEN`.
- **crates.io — secret `CARGO_REGISTRY_TOKEN`.** Create a crates.io token with
  publish rights to `cowrie-rs`, and add it as the repo secret
  `CARGO_REGISTRY_TOKEN`.
- **Go — nothing.** There is no registry. The Go module is "published" purely by
  its VCS tag `go/vX.Y.Z` (see below).

---

## Release flow

### 1. Align the manifests to the version

The `version-check` job fails the run unless **all three** manifests already
declare the tagged version. Bump them together (Go has no version field):

- `tools/pyproject.toml` → `[project] version = "X.Y.Z"`
- `typescript/package.json` → `"version": "X.Y.Z"`
- `rust/Cargo.toml` → `version = "X.Y.Z"`

The tag's `vX.Y.Z` becomes `X.Y.Z` (the leading `v` is stripped) and must equal
each manifest value exactly. Update `CHANGELOG.md` in the same pass.

### 2. Commit on `main`

```sh
git add tools/pyproject.toml typescript/package.json rust/Cargo.toml CHANGELOG.md
git commit -m "Release X.Y.Z"
git push origin main
```

Tag from a commit that is already pushed and green on conformance.

### 3. Push the release tags

Push the release tag **and** the matching Go module subdir tag:

```sh
git tag vX.Y.Z
git tag go/vX.Y.Z          # Go module = the .../go subdir; tag form is go/vX.Y.Z
git push origin vX.Y.Z go/vX.Y.Z
```

> The Go module path dropped its legacy `/v2` suffix, so Go can carry `v0.x` /
> `v1.x` exactly like the other languages. The `go-tag` job only checks the
> `go/vX.Y.Z` tag exists — push it yourself; the workflow does not create it.

### 4. `publish.yml` does the rest

Pushing `vX.Y.Z` triggers the workflow:

1. **`gates`** — builds all four impls and runs `bash tools/run_all_gates.sh`
   (the same 11-gate suite as `conformance.yml`).
2. **`version-check`** — confirms the three manifests match the tag.
3. **publish jobs**, each `needs: [gates, version-check]`:
   - `pypi` → builds sdist + wheel from `tools/` and publishes `cowrie-ref` via
     Trusted Publishing (OIDC, env `pypi`).
   - `npm` → builds `typescript/` and `npm publish --provenance --access public`
     of `cowrie-codec` (uses `NPM_TOKEN`).
   - `crates` → `cargo publish` of `cowrie-rs` from `rust/` (uses
     `CARGO_REGISTRY_TOKEN`).
   - `go-tag` → verifies the `go/vX.Y.Z` tag exists; the tag itself is the
     publish.

After the run is green, the packages resolve as:

```sh
pip install cowrie-ref
npm install cowrie-codec
cargo add cowrie-rs
go get github.com/Neumenon/cowrie/go@vX.Y.Z
```

---

## Freezing to 1.0

Reaching `1.0.0` is a **separate, deliberate step** — not the next routine
release. `1.0.0` declares the wire format frozen: from then on, equal value =>
equal canonical bytes => equal content address is a permanent compatibility
contract across Python, Go, Rust, and TS. Do not bump to `1.0.0` as a version
chore; freeze only when the format is intended to be stable forever. Until then,
keep cutting `0.x` releases.
