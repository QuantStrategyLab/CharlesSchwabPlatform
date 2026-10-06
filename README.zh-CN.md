# CharlesSchwabPlatform

CharlesSchwabPlatform 是 QuantStrategyLab 多仓库交易系统里真正和 Charles Schwab 经纪商 API 打交道的执行 runtime。策略逻辑本身在别处——`UsEquityStrategies` 和 `UsEquitySnapshotPipelines`；这个仓库负责把 runtime-enabled 的策略 profile 接到 Schwab 相关的具体环节：认证、下单、dry-run/live 控制、通知，以及 Cloud Run 部署。如果你要处理 Schwab 连接、执行安全机制，或者这个平台怎么部署和运维，就是在这个仓库里。

[English README](README.md)

> 投资有风险。本项目不构成投资建议，仅用于学习、研究和工程审阅。

## 架构角色

- **层级**：`执行平台`。
- **职责**：Charles Schwab 美股执行运行时。
- **事实源/归属**：Schwab 连接、token/runtime 集成、dry-run/live 控制。
- **消费对象**：UsEquityStrategies、UsEquitySnapshotPipelines artifacts、QuantPlatformKit、QuantRuntimeSettings。
- **禁止事项**：承载策略研究逻辑或把凭据写入 Git。

实盘执行必须先取得持久化原子 claim，失败时关闭执行。Cloud Run 的请求并发数和最大实例数都必须保持为 `1`；原子 claim 负责阻止新旧 revision 重叠时重复向券商提交。未完成的 claim 不会自动过期，必须先核对订单和执行报告，再由人工恢复。

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
