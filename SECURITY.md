# Security Policy


## 中文摘要

- 用途：本文档围绕 `Security Policy`，用于理解 `CharlesSchwabPlatform` 的配置、运行、部署、研究或验收边界。
- 主要覆盖：`Reporting a Vulnerability`、`Secret and Credential Exposure`、`Scope Notes`。
- 阅读顺序：先确认边界、输入输出和权限要求，再执行文档里的命令、CI、dry-run、发布或切换步骤。
- 风险提示：涉及实盘、密钥、权限、Cloud Run、交易所或券商 API 的变更，必须先在测试环境或 dry-run 验证；不要只凭示例直接修改生产。
- 英文正文保留更完整的命令、字段名和配置键；如果摘要和正文不一致，以正文中的实际命令和配置为准。
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
