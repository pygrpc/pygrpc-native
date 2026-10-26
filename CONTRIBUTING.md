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
pip install maturin pytest grpcio grpcio-tools pre-commit
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

- Prefix the title with the type of change, every one that applies:
  `[Bugfix]`, `[Build]`, `[CI]`, `[Core]`, `[Doc]`, `[Misc]`, `[Perf]`,
  `[Test]`. For example, `[Bugfix] Fail a stream cut by close()`.
- Fill in the PR template: what changed, why, and how you tested it. For
  performance changes, include the numbers and how you measured them.
- Keep a PR to one change. Split unrelated changes into separate PRs.
- Address review feedback with new commits on the same branch. PRs are
  squashed on merge, so there is no need to tidy the history.

## What a PR needs to merge

The DCO, lint, rust and python checks have to pass, and a PR needs one
approving review with every review thread resolved. While the project has a
single maintainer, the maintainer's own PRs are merged once the checks pass.
`main` keeps a linear history: PRs are squash-merged with the PR title as the
commit title, and force pushes and branch deletion are blocked. Issues and PRs with no activity
for 60 days are marked stale and closed 30 days later.

## Releases

Releases follow semantic versioning. Existing users call the low-level API
directly, so `tests/test_api.py` pins its names, signatures and event tuples;
change it only together with a new major version.

Before a release:

1. Bump the version in `Cargo.toml` and move the changes into a new section
   of `CHANGELOG.md`.
2. Build the wheel and run the tests of known downstream projects against it.
3. Push a `vX.Y.Z` tag. The release workflow builds the wheels and publishes
   them to PyPI.

## Reporting security issues

Please don't open a public issue. See [SECURITY.md](SECURITY.md).
