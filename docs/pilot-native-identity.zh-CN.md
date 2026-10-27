# 现役部署绑定的原生账户身份核验

`Schwab Pilot Native Identity` 是一次明确派发的只读探测，仅允许原仓库 main 的首次执行，
没有账户、服务、token 或操作覆盖参数。它复用 metadata 入口已使用的原 Variable／Secret
来源、具名 workflow secrets 掩码和固定云身份 bootstrap，不消费独立 daily target。

程序先读取服务、唯一 100% serving revision 和实际 revision 配置。六项身份必须与原受保护
来源一致，实际 revision 的 `runtime_risk_limits.binding.account_hash` 也必须存在且与来源
相同。再次读取服务确认未切换后，才将**实际 revision 中的账户绑定**交给原只读 native
identity 函数；成功后再读一次服务确认仍为同一 revision。服务漂移不能被原账户可访问
这一事实掩盖。

账户 hash 的比较沿用现役 `strategy_runtime` 的十六进制摘要合同：原字符串相等继续有效；
只有双方均为合法的 64 位 ASCII hex 时，才比较其解码后的摘要字节，允许 hex 字母的大小写表示差异。
其他字符串仍要求精确相等，不清洗 key、不做通用 casefold、不接受 Unicode、标点或长度差异。
原生列表中多个相同摘要（包括仅大小写不同）仍为 ambiguous，不接受其中任意一个。
这仅修正只读诊断的表示比较，不修改生产绑定、风险政策、交易配置或旧账本。

原函数只读既有 token Secret 最新版本一次，在内存验证 token 结构和有效期，最多调用一次
Schwab accountNumbers GET。不刷新 token、不写 Secret、不重试、不获取余额／订单／归档，
不通知或下单。token 过期、缺权限、账号不符或请求失败均以固定原因退出，不找新凭据或
修改权限。它只证明配置绑定账户在该 token 可访问的列表中唯一出现，不证明收费、现金、
证券交易权限或账户风险政策合格。

成功路径最多四次 Cloud Run metadata describe；原生请求失败后不再继续读取或恢复操作。
输出及短期工件仅含固定状态、布尔、来源类别和 revision 身份 SHA-256，不含原账户、token、
金额、Secret 引用或错误正文。不公布数字政策摘要。

`PAUSED` 保持原样。`structured_reason_field_present` 仅说明 continuity 中存在非空
`reason_code`、`reason_codes` 或结构化 `reason` 字段，不输出字段内容，不认定其原因有效。
原 continuity 合同并未提供经核验的事故原因，`reason_qualified` 始终为 false。身份通过
也不解除暂停或熔断，不启用真实交易；原因和恢复条件仍需独立核实。

## 续期凭据只读前置检查

`Schwab Runtime Daily Manual` 的 `check_renewal_credentials` 默认关闭，必须单独明确派发。
该模式与 `renew_access_token_once` 互斥，并拒绝其他 daily、发布、身份或采集操作；互斥检查
在云认证前执行。两种模式复用原隔离 job 和 WIF，原 daily job 整体跳过，不加载账户清单。

只读检查仅在内存读取续期原本使用的两项固定 app Secret 的 version 1，每项至多一次、
SDK 不重试，输出 `api_key_status` 和 `app_secret_status` 的固定类别：`ready`、
`permission_denied`、`not_found`、`version_unavailable`、`adc_unavailable`、
`network_unavailable`、`dependency_unavailable`、`invalid`、`load_failed`、`reference_mismatch` 或 `payload_invalid`。
不输出 Secret 值、长度、哈希、异常正文或动态引用；依赖的标准输出、错误输出和日志均抑制。
失败保持 blocked，不能将凭据读取失败解释为 refresh token 无效。

这个模式不读 token，不检查账户，不调用 OAuth，不写 Secret，也不能自动切换到续期。
`refresh_attempts` 与 `secret_write_attempts` 固定为 0。标准续期继续显式启用，复用同一
app 凭据读取与分类逻辑；最多一次 grant 和一次追加，结果不确定时停止，不自动重试或登录。
检查通过只证明指定版本凭据可读取且格式有效，不能证明 OAuth 接受或实盘恢复条件成立。
