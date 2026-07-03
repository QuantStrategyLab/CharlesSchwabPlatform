# CharlesSchwabPlatform

[English README](README.md)

> 投资有风险。本项目不构成投资建议，仅用于学习、研究和工程审阅。

## 这个仓库是什么

CharlesSchwabPlatform 是 QuantStrategyLab 的Charles Schwab 美股执行平台。通过 Schwab token、订单、通知和 Cloud Run 集成执行 runtime-enabled 美股 profile。

它属于执行层，不是策略研究仓库。策略逻辑来自 `UsEquityStrategies`；如果 profile 依赖 snapshot，验证和产物来自 `UsEquitySnapshotPipelines`。

## 运行边界

- 只加载策略包暴露的 runtime-enabled profile。
- 负责券商/API 连接、dry-run 检查、通知和部署配置。
- 凭据必须放在 GitHub Secrets、云密钥系统或券商专用密钥系统中，不能提交到 Git。
- 任何 live 下单路径启用前，都应先从 dry-run 或 paper mode 开始。

## 普通 profile 与 snapshot-backed profile

普通 runtime profile 通常可以直接基于 market history 或 portfolio state 执行。Snapshot-backed profile 需要先从对应 snapshot pipeline 获取当前 artifact bundle，平台才应该执行。平台不应该自行判断策略资格，而应消费策略仓和 snapshot 仓发布的状态与产物。

## 安全部署顺序

1. 在 Git 之外配置 secrets 和 runtime variables。
2. 先以 dry-run 模式运行 workflow 或服务。
3. 检查生成订单、日志、通知和 reconciliation 输出。
4. 确认回滚步骤和 artifact 版本。
5. 上述检查清楚后，再启用定时任务或 live 执行。

## 仓库结构

- `tests/`：单元测试、契约测试和回归测试。
- `.github/workflows/`：CI、定时任务、发布或部署 workflow。
- `scripts/`：运维脚本和本地辅助工具。
- `research/`：研究配置和非 live 候选产物。

## 快速开始

```bash
uv sync --frozen --extra test
uv run --no-sync ruff check --exclude external .
uv run --no-sync python scripts/check_qpk_pin_consistency.py
```

## 延伸文档

- 暂无独立 `docs/` 目录；请先阅读本 README 和 workflow 文件。

## 社区和安全

- 贡献前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)，确认 PR 范围、本地校验和文档要求。
- 讨论、issue 和 review 请遵守 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。
- 涉及密钥、自动化、券商/交易所或云资源的漏洞请按 [SECURITY.md](SECURITY.md) 私密报告；不要为 secret 或实盘风险开公开 issue。

## 许可证

详见 [LICENSE](LICENSE)。
