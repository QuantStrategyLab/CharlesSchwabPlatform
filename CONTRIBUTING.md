# Contributing


## 中文摘要

- 用途：本文档围绕 `Contributing`，用于理解 `CharlesSchwabPlatform` 的配置、运行、部署、研究或验收边界。
- 主要覆盖：`Ground Rules`、`Branching and Pull Requests`、`Local Verification`。
- 阅读顺序：先确认边界、输入输出和权限要求，再执行文档里的命令、CI、dry-run、发布或切换步骤。
- 风险提示：涉及实盘、密钥、权限、Cloud Run、交易所或券商 API 的变更，必须先在测试环境或 dry-run 验证；不要只凭示例直接修改生产。
- 英文正文保留更完整的命令、字段名和配置键；如果摘要和正文不一致，以正文中的实际命令和配置为准。
Thanks for contributing to `CharlesSchwabPlatform`.

## Ground Rules

- Prefer small, low-risk pull requests.
- Keep refactors separate from behavior changes.
- Add or update tests when changing runtime behavior.
- Do not use deployment or scheduled workflows as a substitute for local verification.

## Branching and Pull Requests

- Create a topic branch for each change.
- Open a pull request with a short summary and a concrete test plan.
- Wait for CI to pass before merging.

## Local Verification

Run the main verification command before opening a pull request:

```bash
python3 -m pip install -r requirements.txt && python3 -m unittest discover -s tests -v
```
