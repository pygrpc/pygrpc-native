# Contributing

Thanks for helping out! Bug reports, fixes and new features are all welcome.

## Before you write code

For a new feature or a bug you found, open an issue first so we can agree on
the approach. Small fixes such as typos can go straight to a pull request.

## Development setup

You need a Rust toolchain (1.88 or later) and Python 3.10 or later.

```bash
git clone https://github.com/<you>/pygrpc-native && cd pygrpc-native
python -m venv .venv && source .venv/bin/activate
pip install maturin pytest grpcio pre-commit
maturin develop --release
pre-commit install
```

## Checks

CI runs the same checks, so run them before you push:

```bash
pre-commit run --all-files                 # rustfmt, clippy, ruff, codespell
cargo test --locked --no-default-features  # Rust unit tests
maturin develop --release && pytest tests  # Python tests
```

New behavior needs a test, and a bug fix needs a test that fails without it.
Write tests against the documented behavior rather than the implementation.

## Commits

Every commit must carry a `Signed-off-by` trailer certifying that you agree
to the [Developer Certificate of Origin](DCO). `git commit -s` adds it:

```bash
git commit -s -m "Describe the change"
```

If you forgot, `git commit --amend -s` fixes the last commit and
`git rebase --signoff main` fixes all of them. The DCO check fails on any
commit without it.

## Pull requests

- **Title**: prefix it with the type of change, and include every prefix
  that applies: `[Bugfix]`, `[Build]`, `[CI]`, `[Core]`, `[Doc]`, `[Misc]`,
  `[Perf]`, `[Test]`. For example, `[Bugfix] Fail a stream cut by close()`.
- **Description**: fill in the PR template. Say what changed, why, and how
  you tested it. For performance changes, include the numbers and how you
  measured them.
- Keep a PR to one change. Split unrelated changes into separate PRs.
- Address review feedback with new commits on the same branch. PRs are
  squashed on merge, so there is no need to tidy the history.

## Releases

Releases follow semantic versioning. A maintainer bumps the version in
`Cargo.toml` and pushes a `vX.Y.Z` tag; the release workflow builds the
wheels and publishes them to PyPI.

## Reporting security issues

Please don't open a public issue. See [SECURITY.md](SECURITY.md).
