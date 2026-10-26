# Security Policy

Please don't report security vulnerabilities in public issues, discussions or
pull requests.

## Reporting a vulnerability

Use **Report a vulnerability** on the
[Security tab](https://github.com/pygrpc/pygrpc-native/security), which
only the maintainers can see. It helps to include:

- the affected version or commit
- a description of the issue and its impact
- steps to reproduce, or a proof of concept
- any suggested fix

You should hear back within a few days. Please keep the issue confidential
until a fix is released or 90 days have passed, whichever comes first. We
will publish a GitHub Security Advisory with the fix and credit you unless
you ask us not to.

## Supported versions

Fixes go into the latest release only. Please upgrade to get them.

## Scope

pygrpc-native has no TLS or authentication yet, so keep it on localhost or a
private network. Crashes, memory corruption or unbounded memory use caused by
a single client are still in scope.
