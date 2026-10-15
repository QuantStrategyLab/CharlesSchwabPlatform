# Security Policy

Thanks for helping keep `CharlesSchwabPlatform` safe.

This repository is part of a runtime trading platform. Please do **not** open a public issue for vulnerabilities involving credentials, broker access, cloud resources, order execution, or secret material.

## Reporting a Vulnerability

- Contact the maintainer directly at GitHub: `@Pigbibi`.
- If private vulnerability reporting is enabled for this repository, prefer that channel.
- Include the repository name, affected commit or branch, environment details, and exact reproduction steps.

## Secret and Credential Exposure

If you suspect tokens, passwords, API keys, service-account keys, or broker credentials were exposed:

1. Rotate the exposed secrets immediately.
2. Pause scheduled jobs or deployments if the exposure can affect automation or trading behavior.
3. Share only the minimum evidence needed to reproduce the issue.

## Scope Notes

Security fixes should stay minimal and focused. Please avoid bundling unrelated refactors with a security report or patch.
