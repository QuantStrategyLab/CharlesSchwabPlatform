# Serving Telegram 只读身份诊断

入口为 `notify-route-check.yml` → `notify-route-check-protected.yml` → `scripts/notify_route_check.py`。仅接受 main 分支的手动 workflow_dispatch；不改生产设置、secret、IAM、Cloud Run、通知路由、交易或预算。

## 实际读取与证明范围

- 读取既有服务配置；private list/target JSON/region 与既有 GCP 设置以同值 reusable secrets 传入，在 runner header 前注册掩码。不新建生产 secret，不打印 `add-mask` 命令或原值。权限仍为原 `contents: read` 与 `id-token: write`，沿用原 WIF / SA。
- 从 Cloud Run **status positive traffic** 逐 revision describe 其实际 containers/env；不把 latestReady 或 service template 当 serving。要求流量记录合法且覆盖 100%，不猜缺失 revision。多流量/split 仍读取各 revision，但全体与每行 bot 比较都保持 unknown/null；重复或不完整流量也 unknown。
- 只有单一 container 的 TELEGRAM_TOKEN 可以解析。明确 plain env 仅在内存用于 getMe；duplicate/缺失/非法引用不猜。该 runtime selector 不使用 TG_TOKEN；主 token 无效或探测失败时不借次优 token 救成功。
- secretKeyRef 的版本必须是精确正整数。读取绑定项目/secret/该固定版本；支持完整 resource 及 `run.googleapis.com/secrets` 的明确 alias→完整 resource 映射。错误/重复 alias 不回落到同项目同名 secret。**latest/其他版本别名不能证明正在运行进程加载的 immutable token，故保持 unknown，不自动改绑定。**
- 比较参考是**本项目**既有 `quant-sentinel-telegram-bot-token` 当次 latest 读取后 getMe 的 bot 身份。它不是中央已批准 canonical reference，也不因 secret 名相同证明跨项目相同。两端 getMe 只 GET，检查 ok=true、is_bot=true、有效正整数 id；缺失、读失败、网络/坏响应均 null/unknown，不写 false。仅两端身份都已知且 sole-serving 无歧义时输出 match/true 或 mismatch/false。
- 所有 gcloud 命令 stdout/stderr 捕获，30 秒超时；getMe 15 秒超时并限制响应 64 KiB。没有 update、sendMessage、broker、真实业务或交易调用。服务/引用值与 token 只在私有内存中使用。

这是一次读取快照的 Telegram **配置绑定**比较；不能由诊断 source SHA 反推 runtime 镜像、已安装代码、有效 channel 或真实交付。服务若正改变流量，需重新观察稳定状态，不把单次快照当原子发布证明。

## 公开输出

stdout 和 GitHub summary 使用 `qsl.notify_route_identity.v1`：固定 status/reason enum、nullable bool、`service#N` / `revision#N` ordinal，以及经 40 位小写 hex 校验的诊断 `source_sha`。不输出 token/chat/hash/last4、project/service/revision/SA/secret 引用/版本、bot 原 ID/name、自由错误/URL。没有公开私有文件 artifact。

`status=ok` / exit 0 仅表示诊断形成结果，**不表示 route 已就绪**。看逐服务 `bot_status=match|mismatch|unknown`、`same_bot_as_contract=true|false|null` 与固定 reason。scope 始终 `project_contract_only`，`canonical_route_status=unknown`。

## 尚未验收的入口

`same_chat_as_canonical` 和 `same_topic_as_canonical` 固定 null、对应 status unknown：当前没有批准的中央 private canonical reference，本批不创造新 secret、全局路由规则或 topic 值，也不额外读取 chat secret。既有 runtime chat 为 QSL_GLOBAL 优先 GLOBAL；heartbeat/guard 只遍历 GLOBAL 全列表；中央日报优先 QSL_GLOBAL/GLOBAL/plugin 且只取首 ID。它们需要按各自原 selector 在受保护比较边界取得三态证据，不能重新统一解析来使其匹配。

本诊断没有核验 GHA heartbeat/guard 的注入、scanner/plugin/drill/VPS 实际安装或中央 Environment；本仓所查发送器未给 message_thread_id，不等于自然消息已经进入同一 topic。same bot 不等于相同 chat/topic、shared dedup 或 exactly-once。保留既有 alert/订单可达与 quiet，不切换 dedup mode。

B7/B8 的中央生产 reader/sections_v2 采用、批准 producer/runtime/source 身份、自然周期安全 receipt 和最终 Telegram 截图仍是独立验收项；不会用只读 success、合成夹具或旧截图代替。

## 本地验证

```bash
uv sync --frozen --extra test
PYTHONPATH=. PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 uv run --no-sync python -m pytest -q tests/test_notify_route_check.py
uv run --no-sync ruff check --exclude external .
PATH=/path/to/shellcheck:$PATH actionlint
```

回归仅 fake gcloud/urllib 与固定合成路由；还应运行 CONTRIBUTING 全量与锁/pin/启动检查。源码工程验收后由主智能体在最终 main 执行受控只读诊断，结果依真实 read/getMe 状态分项记录；不把缺失事实补成匹配。
