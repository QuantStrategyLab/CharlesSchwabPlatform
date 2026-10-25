# 真实试点前的部署元数据读回

手动运行 `Schwab Pilot Metadata Readback`，仅允许原仓库 `main` 的首次执行，不自动重跑，
没有可填写的服务、账户或政策参数。
它沿现有 `CLOUD_RUN_SERVICE_TARGETS_JSON`、`RUNTIME_TARGET_JSON` 的 Variable 优先、Secret
回退规则，以及原 `CLOUD_RUN_SERVICE` Secret 和 region 配置读取一个明确选中的服务。
调用端将值作为具名 workflow secrets 传给专用读取端，真实绑定不会进入未掩码的 job env
或步骤头；派生的私有标识也会在 runner 内掩码。

云项目和 WIF 身份不在新入口复制。bootstrap 只解析原
`.github/workflows/env-sync-drift-check.yml` 的三项顶层固定配置，掩码后写入 runner 环境，
不接受其他源路径或自定义覆盖。原源缺失、重复或不再是支持的固定格式时，以固定原因码停止。

读取顺序为服务、唯一 100% 承接流量的 revision、该 revision 元数据、再次读取服务。
不使用 latestReady／latestCreated 或零流量候选替代 serving；账户、服务或来源歧义及前后
serving 变化均停止。复用原同步计划的纯解析函数，不构建完整同步计划、不拉取远端策略配置。

输出和 `schwab-pilot-metadata-safe` 工件仅含固定状态、布尔值及 serving revision 身份摘要。
账户／服务原文、Secret 引用、环境配置、数字政策、上游错误和完整报告均不输出。环境字段的
缺失、空白、Secret 引用、非法值与显式 false 分开保留，不补默认值。

`verified_metadata` 仅表示所读 serving 的身份与既有声明匹配且读前读后稳定。各项实际启用、
dry-run、cash-only、执行通道、continuity、release 和风险政策的声明单独显示；即使声明
存在或与来源一致，也不代表金融政策合格、现金可支用、运行健康或允许下单。

本入口只执行三次 Cloud Run metadata describe，无券商、业务端点、GCS 报告、云日志、通知、
Secret payload 读取或云写入，不重试、不解除熔断。服务原本可能处于 enabled／live，但本入口
不会触发它执行。首次真实运行需先合并来源并由授权执行者从准确 `main` 版本派发；本地合成
测试通过不能代替真实读回证据。
