# Schwab DIGEST_CANDIDATES 生产者（P0-05）

审计日：2026-10-09  
仓库：CharlesSchwabPlatform  
中央消费：QuantRuntimeSettings `daily_digest_aggregator` / Environment `runtime-strategy-switch`

## 目标

从**已验证**的 Schwab runtime daily 投影（及可选 account-facts）生成中央日报可消费的候选 JSON，经受保护通道注入 QRS，**不**默认上传公开 Actions artifact，**不**自动改 QRS 生产 Environment。

## 候选合同（与 QRS #586 对齐）

`platform_id` 固定为 **`schwab`**（不是 `charles_schwab`）。

最小样例（脱敏；数字仅为合成夹具）：

```json
{
  "schema_version": "qsl.digest_candidates.v1",
  "runs": [
    {
      "platform_id": "schwab",
      "strategy_profile": "soxl_soxx_trend_income",
      "opaque_account_uid": "acct_opaque_example",
      "target_id": "schwab/example-target",
      "actually_ran": true,
      "fill_count": null,
      "order_count": null,
      "cycle_count": 1,
      "field_status": {
        "fill_count": "counts_unknown",
        "order_count": "counts_unknown",
        "cycle_count": "known"
      },
      "evidence_provenance": "candidates",
      "reason_code": "schwab_fills_not_connected",
      "signal_summary": "no_signal",
      "rebalance_kind": "no_rebalance",
      "status": "ok"
    }
  ],
  "producer_status": "projected",
  "producer_reason": ""
}
```

### 字段来源与未知项

| 字段 | 来源 | 未知时 |
| --- | --- | --- |
| `platform_id` | 固定 `schwab` | — |
| `strategy_profile` | daily `record.target.strategy_profile` | 回退文档化默认 `soxl_soxx_trend_income`（sole target） |
| `opaque_account_uid` | 受保护 env / binding `account_hash`（已不透明） | 空字符串 + `opaque_account_uid_absent` |
| `target_id` | `SCHWAB_DIGEST_TARGET_ID` 或 `SCHWAB_ACCOUNT_FACTS_TARGET_ID` | 空字符串 + `target_id_absent` |
| `actually_ran` | 有 covering run activity 才为 `true` | 无 activity → 不产出 run |
| `fill_count` / `order_count` | daily `fills`；当前 `source=not_connected` | **必须** `null` + `counts_unknown`；禁止写成 0 |
| `cycle_count` | covering run 数 | — |
| `equity` | 可选 account-facts `broker_reported_balances[].net_assets`（archive 只读投影；selector 接受 legacy `["live"]` 或 native `[account_hash]`） | 省略字段；不猜。**不要求** covering runs：无 run 时仍可产出 `actually_ran=false` 权益行 |
| `holdings` | 当前 daily / facts 投影**无**持仓明细 | 省略；不猜 |
| `signal_summary` / `rebalance_*` | 由 run `activity` 映射 | 无 activity 则整行不产出 |

## 本机投影（合成 / 已有 JSON）

```bash
python3 scripts/project_digest_candidates.py \
  --daily-projection /path/to/daily-projection.json \
  --account-facts /path/to/optional-account-facts.json \
  --opaque-account-uid 'acct_opaque_example' \
  --target-id 'schwab/example-target' \
  --output /tmp/schwab-digest-candidates.json
```

stdout 只打印安全摘要（`status` / `runs` 计数），不含 uid、target、权益金额。

单测：

```bash
python3 -m pytest tests/test_project_digest_candidates.py -q
```

## Workflow（可选，默认关）

`.github/workflows/runtime-daily-sync.yml` 增加输入 `emit_digest_candidates`（默认 `false`）。

开启后在 prepare 或 publish **成功路径之外**另跑 `scripts/emit_digest_candidates.py`：

- 复用同一 WIF / report prefix / runtime target
- 写出到 `$RUNNER_TEMP/schwab-digest-candidates.json`（ephemeral）
- 权益：对同一 archive **只读**调用 `project_schwab_account_facts_history`（**禁止** POST account-facts / broker）；成功则写 `$RUNNER_TEMP/schwab-digest-account-facts.json` 并注入候选 `equity`
- 也可显式设 `SCHWAB_DIGEST_ACCOUNT_FACTS_PATH` 指向已有 facts JSON（优先于 archive 投影）
- **不** `actions/upload-artifact`
- stdout / 摘要仅安全字段：`equity_present`、`runs`、`fill_count_null`、`account_facts_source`、`identity_*_present`（无 uid、无金额）

所需受保护配置（不得写入公开仓）：

| 名称 | 用途 |
| --- | --- |
| `SCHWAB_DIGEST_OPAQUE_ACCOUNT_UID` | 可选；缺省回退 binding `account_hash` |
| `SCHWAB_DIGEST_TARGET_ID` | 优先；否则 `SCHWAB_ACCOUNT_FACTS_TARGET_ID` |
| `SCHWAB_ACCOUNT_FACTS_SERVICE_NAME` | archive 权益投影所需 service 名 |
| `SCHWAB_NET_ASSETS_CURRENCY` | 必须为 `USD`（owner-confirmed）；否则省略 equity |
| `SCHWAB_CASH_CURRENCY` | 可选；`USD` 时 facts 可带 cash（digest 仍只取 net_assets） |
| 既有 daily 所需 secrets/vars | 与 `runtime-daily-sync` 相同 |

可选私有 artifact（默认关；仅授权灌 QRS 时开，保留 1 天）：

```bash
gh workflow run "Schwab Runtime Daily Manual" -R QuantStrategyLab/CharlesSchwabPlatform --ref main \
  -f emit_digest_candidates=true -f capture_digest_candidates_artifact=true
```

Dry-run emit（不改 QRS Environment、不发 Telegram）：

```bash
gh workflow run "Schwab Runtime Daily Manual" -R QuantStrategyLab/CharlesSchwabPlatform --ref main \
  -f emit_digest_candidates=true
# 日志检查 equity_present / fill_count_null / runs；无公开 artifact
```

## 如何注入 QRS（人工，不自动改生产 Environment）

中央文档：QuantRuntimeSettings `docs/digest-candidates-wiring.zh-CN.md`（PR #586）。

推荐步骤：

1. 在本仓用合成 fixtures 或获准的手动 `emit_digest_candidates` 得到候选 JSON。
2. 确认 JSON **无**原始账户号、无 token；`fill_count`/`order_count` 为 `null` 而非 `0`（除非未来 fills 真正接线且已验证）。
3. 由有权限的维护者把整份 JSON 写入 QRS 仓库 Environment **`runtime-strategy-switch`** 的 secret **`DIGEST_CANDIDATES_JSON`**（或私有前置步骤写出文件后设 `DIGEST_CANDIDATES_PATH`）。
4. 在 QRS 对 `daily-digest-notify.yml` 做 `workflow_dispatch`（默认 `dry_run=true`），检查 receipt：
   - `source_coverage.candidates_loaded=true`
   - Schwab run 的 `field_provenance.fill_count` 非「已验证零」
   - 文案成交为「未知」而非「无成交」
5. 确认 dry-run 后再考虑关闭 dry-run；**本 Schwab PR 不会自动写入 QRS Environment**。

也可将 ephemeral 文件同步到私有 GCS，再由受保护 job 注入；同样禁止默认公开 artifact。

## 与 quant / 其他平台边界

- 不改 LB-HK、Firstrade sync、Cloud Run ingress、生产策略或风险预算。
- 不刷新 Schwab token、不 POST account-facts / runtime-daily（emit 路径只 prepare + 只读 facts 投影 + 本地写 ephemeral）。
- IBKR 若后续更易接线，另开平台 PR；本任务只做一个平台（Schwab）。

## 未知项（基线）

- 生产 `SCHWAB_DIGEST_TARGET_ID` / opaque uid 是否已与控制台 binding 一致：未知（待维护者填）；缺省回退 binding hash + `SCHWAB_ACCOUNT_FACTS_TARGET_ID`
- archive 当日报告是否落在 account-facts `MAX_AGE`（36h）窗口内：未知；窗外则 `equity_present=false`，不编造
- daily `zero_run_scope_target_scope`（无 covering runs）与 digest 权益解耦：权益走 facts；日报 scope 另查
- holdings 仍无源：省略
- fills 何时从 `not_connected` 升级为可计数：未知；升级前禁止写 0
- QRS `DIGEST_CANDIDATES_JSON` 仍须人工注入；emit **不会**自动改 Environment
