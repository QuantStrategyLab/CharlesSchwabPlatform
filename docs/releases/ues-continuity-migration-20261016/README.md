# UES 迁移准备：CharlesSchwabPlatform

这是文档草稿，供审查候选版本和迁移入口设计；不得直接合并为生产迁移批准。
当前源码版本声明和 lockfile 均保持原样，本 PR 不接通发布路径。

Schwab 使用 RUNTIME_TARGET_JSON；未来只在正式迁移入口中更新完整 release identity、新 continuity 和既有 risk binding 的 UES revision。

从健康旧 UES `4a3943883cd6b5bbfe32a559e56a91b40a81b7ce` 直接选择候选 `76e04b18180f21f7e04bd1f6e116a3206e2c30fc`，包含 #607，但本次不启用 QNDX。
QPK override 保持 `bfe55791adcd8867790e15dba6fd35304e4093c0`。候选日期 2026-10-19 尚未批准。
两个平台各用自己的 profile manifest；具体账户 selector、冻结旧 target、完整回滚 tuple 和原始 variable 只保存在受限审批材料中，不上传公开仓库。

- [入口设计、lineage 与回滚](MIGRATION_ENTRY_DESIGN.md)
- [交易差异、限额与验证边界](TRADING_DIFF_REVIEW.md)
- [全部 214 个提交的分类](COMMIT_REVIEW.tsv)
- [固定来源与检查结果](REVIEW_STATUS.json)

必须在正式申请应用批准前补齐：同 source revision 的合格 release inputs/evidence；实现并验证迁移入口与旧 LKG 回滚；确认 TQQQ 有效限额来源；发行完整新 manifest、target/image/source 摘要及内容绑定批准。
本 PR 的 CI 只检验文档所在源码；即便通过，也不替代这些条件。
