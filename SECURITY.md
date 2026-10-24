# Security Policy

**Please do not report security vulnerabilities through public GitHub issues,
discussions or pull requests.**

## Reporting a vulnerability

Use GitHub's private vulnerability reporting: go to the
[Security tab](https://github.com/theminjunchoi/pygrpc-native/security) and
click **Report a vulnerability**. Only the maintainers can see the report.

Please include what you can of:

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

pygrpc-native has no TLS or authentication yet, so it is meant for trusted
networks: bind it to localhost or a private network and restrict the port.
Reports that rely on exposing it to untrusted clients are still welcome when
they show something worse than what an unauthenticated gRPC server allows,
such as memory corruption, a crash, or unbounded memory use from a single
client.
