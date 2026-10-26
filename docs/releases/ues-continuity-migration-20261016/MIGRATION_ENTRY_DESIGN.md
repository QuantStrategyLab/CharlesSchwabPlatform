# UES continuity migration entry：供审查的设计

状态：`DESIGN_REVIEWED / NOT_IMPLEMENTED / NOT_APPLY_READY`。2026-10-16 SGT。
本稿仅定义后续实现和验收条件；现有发布 workflow 不具备这个入口，不能据此立即 dispatch。

## 固定范围与冻结规则

旧 UES：`4a3943883cd6b5bbfe32a559e56a91b40a81b7ce`。
候选 UES：`76e04b18180f21f7e04bd1f6e116a3206e2c30fc`，即 #607 的正常 merge commit；不跟踪 main。
有效 QPK 维持 `bfe55791adcd8867790e15dba6fd35304e4093c0`。
本轮只迁移既有 Schwab SOXL 和 IBKR TQQQ 两个 incumbent target；QNDX 仅随包具备能力，默认不启用，不新增 DCA target。
IBKR 同仓库的其他账户、SOXL target、RECONCILE_ONLY 条目均不加入本次集合。

总计划 ID：`ues76e04b1-two-incumbents-20261019`。两个策略必须各自生成完整 manifest，不能共用一份 profile 不匹配的 manifest。

| 项目 | Schwab | IBKR |
|---|---|---|
| profile | `soxl_soxx_trend_income` | `tqqq_growth_income` |
| target_set_id | `ues-76e04b1-schwab-existing-live` | `ues-76e04b1-ibkr-existing-live` |
| proposed release_id | `schwab-ues-76e04b1-20261019` | `ibkr-ues-76e04b1-20261019` |
| proposed baseline_id | `schwab-soxl-ues76e04b1-20261019` | `ibkr-tqqq-ues76e04b1-20261019` |
| baseline_kind | `release_attested` | `release_attested` |
| 初始 candidate state | `RECONCILE_ONLY` | `RECONCILE_ONLY` |

**提议生效交易日为 2026-10-19，按 America/New_York 解释；它不是已获批日期，也不会自动生效。**
正式执行前必须按适用交易所日历确认该 session。若正式验收赶不上该日，必须改日期、重新生成 manifest/identity/target fingerprint 和批准材料，不能沿用旧批准。
`captured_at` 填真实冻结日；不能把计划生效日或旧 baseline 的 2026-08-30 冒充实际捕获日。

`targets` 必须从受限计划取精确 platform/service selector。公开文档中的 `schwab:existing-live`、`ibkr:existing-live` 只是别名，不能直接用于最终 release。
私有计划须逐个保存旧 revision 全名、image digest、source SHA、完整旧 target、旧 baseline ID/hash、旧 release identity、变量当前摘要及更新时间。

## 新基线如何生成

1. 为每个 profile 提供经过接受的 canonical config、risk-policy、同候选 UES source revision 的 promotion-eligible evidence，以及不可变且获准的 plugin 文件集合。源码 diff、单测和本次合成案例不能冒充这些输入。
2. 调用固定 QPK 的 `assess_strategy_release_readiness()`；必须 `is_ready=True`，再调用无参数 `build_manifest()`。该固定 API 不接收 lineage keyword；用 `dataclasses.replace(manifest, supersedes=old_release_id, rollback_to=old_release_id)` 生成最终完整 manifest，然后再取最终 hash/identity。从真实文件字节和官方 bundle 算法生成摘要，不沿用旧 evidence/manifest 摘要。
3. 完整 manifest 的 `strategy_profile / strategy_revision / target_set_id / targets / effective_session / supersedes / rollback_to` 全部绑定本次内容。用其 `runtime_identity()` 替换整个 `strategy_release`，不能只换 revision。
4. 从健康旧 target 的深拷贝生成新 target；Schwab 另改 `runtime_risk_limits.binding.ues_revision`。保留风险数值、账户选择、profile、模式、日历、时区、scheduler、全部非本次字段。IBKR 不顺手添加原来不存在的风控字段。
5. 仅在证据与授权条件均满足后，计算 QPK `runtime_target_fingerprint(new_target)`，即移除整个 `live_continuity` 后按 sorted/compact/ASCII JSON 做 SHA-256。写入全新的 `release_attested` continuity，初始 `RECONCILE_ONLY`；立即以原 `assert_matches_target()` 和完整 runtime-target parser 验证。
6. 新 baseline 的确切 hash 在这些材料齐备后才能冻结。当前没有发行新 manifest、runtime candidate 或新 live baseline hash；非可加载蓝图中的 `null` 禁止提交到运行环境。不能为填完审批表制造一个看似合格的 hash。

lineage 同时记录在完整 manifest 的 `supersedes / rollback_to` 和私有迁移记录的 `old_baseline → new_baseline`，不往 compact runtime identity 或 continuity 塞 schema 不接受的 lineage 字段。

## 合法 admission 与发布顺序

保留普通发布路径的 continuity、pin、镜像绑定和 HTTP startup gate 原义。新增的是单独的受审查 transition validator，不是 `skip_gate`、`allow_mismatch`、临时改 env 或把任意当前配置重新记为 baseline。

拟议接口：受信任 main 上的 `UES Continuity Migration` workflow，精确绑定 `direction=forward|rollback`、`phase=verify|stage|promote|record`、完整 source SHA、私有 migration artifact digest、approval artifact digest、selected service、候选 revision/image digest。此接口本次未实现，禁止把这些名字当成已有可运行命令。

顺序必须是：

1. **Verify old**：从不可变、已授权的旧 revision 读取 target，原 gate 验证其 fingerprint；精确核对完整 revision/image/source/release/selector tuple、100% incumbent routing 及无并发变更。已漂移的 service template 和 GitHub variable 只作为“待替换输入”比对，不可充当旧授权基线。任何额外漂移都停止。
2. **Verify new**：离线验证新 target 自洽、manifest/release-evidence parity、完整 source/lock/pin/installed UES 一致、QPK override 不变。精确字段 diff 只允许整个新 release identity、Schwab UES risk binding 和新 continuity；禁止放宽 0.679 / 0.97 / 0.03，禁止新增 QNDX 开关。
3. **Authorize edge**：批准的 artifact 必须绑定旧 tuple、新 manifest hash、新 target hash、两个精确 target、effective session、固定源码与回滚 tuple；从受限且已有的可信审批来源取回，校验审批权限和内容摘要。PR 文本、输入 `approved=true` 或自报 actor 不算授权。若现有权限无法提供该验证，停止，不改 IAM 或扩大权限。
4. **Stage**：固定 source 构建后按镜像 digest 校验实际安装 UES；只把新 image 与新 target 一起装入选定服务的无默认流量 candidate。从冻结旧 revision 允许的配置生成，不能继承当前坏 template 的 target。仅替换必要的 target env，不做全量 env/Secret 同步。候选保持 `RECONCILE_ONLY`，无 scheduler/tag 指向其执行入口；验证无订单能力后才能做需要的只读/对账探测。
5. **Attest**：原 `verify_target_continuity()` 验证 candidate；原 `verify_traffic_revision()` 验证完整 revision/source、Ready、ContainerHealthy、HTTP `/health:8080`。继续校验 digest、installed UES、loaded release receipt 和只读业务/对账状态。单凭 HTTP 200 不算业务验收。阶段性验证只用于新 candidate，不能拿旧 revision 的结果顶替。
6. **Promote**：用户批准同一新 tuple 的激活和切流后，另建只改变 continuity state 为 `ACTIVE_LKG` 的最终无流量 revision，重新跑原 candidate gate。baseline fingerprint 不变，因为 state 不参与其计算；revision/image/source tuple 仍须精确回读。按先 Schwab、验收后 IBKR 顺序推进；第二台失败时按批准的成组回滚政策恢复已推进的一台，不留下未批准的混合状态。
7. **Record**：变量在 promotion 前保持旧值/原已知漂移值，不作为 staging 来源。最终切流成功后，在同一受控入口中 compare-and-swap 写入精确批准的 target；IBKR 按唯一 service selector 更新一个 inventory 条目，证明其他条目完全不变。变量与服务操作不能真正跨系统原子提交，必须记录每步结果、做 readback，失败则停止并按已批准补偿步骤回滚。写入结果未知时先查状态，不盲目重试。

普通 workflow 此后只接受新 baseline 自洽且 pin 一致的配置。旧模板不满足 gate 时必须显式报告它与旧 incumbent 的差别，不通过切到 `RECONCILE_ONLY` 为旧目标伪造迁移资格。

## 回滚设计

回滚目标严格为本次私有记录中的 Schwab `00781-lel` 和 IBKR `n0110100718`，不是 latest、latest-ready 或模板 revision。
保留两个旧 revision、原镜像与 target/continuity/release 完整记录；迁移期间不 cleanup，不改或重建旧镜像。

触发条件：任何 release/installed-package/指纹/限额/账户不一致；新 revision 非 Ready；HTTP 验收失败；对账失败；出现未知 pending/fill 或反重发冲突；任一平台状态写入结果不明。

1. 停止后续 promotion。若 candidate 已承接执行，先使用已授权的正常暂停能力阻止新增执行，等待/核对 in-flight、pending、fills 和 T+1/anti-resend 状态；不删执行记录、不强制重跑、不补发订单。当前默认仍是旧 revision 的 stage 失败只保留失败候选作证，不切流。
2. 回滚入口验证批准的 `new→old` edge 与旧完整 tuple，确认旧 revision 仍存在、Ready/ContainerHealthy、实际旧 target 匹配原冻结 hash，再精确恢复旧默认流量。
3. **本次元数据回读确认两个旧 LKG 都是 TCP startup probe。** 现有新候选 HTTP-only gate 不会接受直接切回；必须在未来入口实现中审查只接受批准旧 immutable tuple 的 rollback attestation，仍检查 readiness 和适当的只读健康回执。它不能成为任意 TCP revision 的放行条件，不能删除或跳过 forward candidate 的 HTTP gate。该实现和测试缺失时，不允许开始迁移。
4. 回读默认流量和业务状态后，在批准的补偿步骤中把选中的 GitHub variable target 恢复为受限保存的**健康旧 target**。当前变量已漂移，不能把执行前的坏值误当成业务回滚目标；另保存其 CAS 摘要用于检测并发。IBKR 不覆盖整个 inventory。
5. 依赖源码若仍指向新 UES，禁止直接用普通 image deploy；后续若需重新构建旧基线，另经正常 PR 恢复旧 pin/lock 并处理已知 C4 兼容问题。紧急路由回滚只重用旧 immutable revision，不以源码回退冒充镜像回滚。

本次没有执行上述任何生产步骤，没有提供可直接运行的裸 `gcloud update` 或变量写命令。

## 设计复核

| 检查 | 结论 |
|---|---|
| 旧健康 target / 当前 pin-only mismatch | 原函数分别接受 / 拒绝；本地复核通过 |
| source 改动但沿用旧 hash | 原函数拒绝；本地复核通过 |
| schema 外 lineage 字段 | 原函数拒绝；本地复核通过 |
| 新 self-consistent fixture / freeze 后篡改 | 原函数接受 fixture / 拒绝篡改；仅说明 schema/hash 行为 |
| state-only promotion | hash 不变；并不自动授予切流权限 |
| 缺 canonical release 输入 | 官方 readiness 拒绝，`build_manifest()` 拒绝；本次没有 loadable candidate |
| approval artifact / CAS / 无订单 staging / 回滚旧 TCP tuple | 已定义验收条件；入口尚未实现，尚无端到端通过证据 |

实现验收还必须覆盖：无批准、伪造批准、过期/错日期、目标集扩大、选错 service、改限额、改 QPK、旧 artifact 重放、错误镜像或 source、并发变量变化、部分成功补偿、未知写结果、in-flight 订单、任意 TCP rollback、候选受 scheduler/带 tag URL 调用。任何一项都不得通过捕获错误后返回成功来解决。
