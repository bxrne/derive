# Contributing

Thanks for contributing to **derive**.

## Development setup

- Install **Zig 0.16.0-dev** or newer (this project tracks Zig master).
  - CI currently pins `0.16.0-dev.3028+a85495ca2` in `.github/workflows/ci.yml`.
- Optional (docs only): install [`uv`](https://github.com/astral-sh/uv) (used to serve generated docs locally).

## Build, test, and demos

```sh
zig build
zig build test
zig build demo-contiguous
zig build demo-tree
```

## Documentation

Generate docs (CI runs this too):

```sh
zig build docs
```

Serve the generated docs locally:

```sh
cd docs/libderive
uv run python -m http.server 8000
```

## Code style

- **Formatting**: run `zig fmt .` before pushing.
- **Design constraints**: this codebase follows TigerStyle; see `DESIGN.md` (and the linked TigerStyle document) for the rationale and non-negotiables.
- **Dependencies**: keep it **zero-dependency** (Zig stdlib only) unless there is a very strong justification.

## Changes, PRs, and CI

The system shall keep `main` green: open a PR for changes and ensure CI passes.

CI runs:

- `zig build test`
- `zig build demo-contiguous`
- `zig build demo-tree`
- `zig build docs`

## Commit messages (required for releases)

Releases are automated on `main` via Semantic Release (see `.releaserc.js`). The system shall use **Conventional Commits** so versioning and `CHANGELOG.md` generation work.

Examples:

- `feat: add tree-backed permutation store`
- `fix: truncate WAL on checksum mismatch`
- `docs: expand design notes for matching`
- `test: add replay corruption cases`
- `refactor: simplify scan plan scoring`

Add `!` for breaking changes, e.g. `feat!: change dataset open API`.

## Reporting issues / proposing changes

- **Bugs**: include Zig version, OS/arch, repro steps, expected vs actual behavior, and any logs/output.
- **Performance**: include dataset size, benchmark command, `-Doptimize=ReleaseFast` vs debug, and timing output.

