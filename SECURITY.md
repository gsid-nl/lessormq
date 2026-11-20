# Security

## Reporting a vulnerability

Please do not open a public issue for a security problem. Report it privately through
[GitHub's private vulnerability reporting](https://github.com/gsid-nl/lessormq/security/advisories/new),
or by e-mail to security@gsid.nl.

Describe what you found, how to reproduce it, and what an attacker could do with it. We
answer within five working days, keep you informed while we fix it, and credit you in
the advisory unless you prefer not to be named.

## Supported versions

Security fixes go into the latest release. Until 1.0, there is no long-term support for
older versions.

## Deployment

Things to get right when you run LessorMQ yourself are in
[self-hosting](docs/self-hosting.md): expose only `/v1` and `/healthz`, and keep
`/metrics` for your monitoring.
