# 现役部署绑定的原生账户身份核验

`Schwab Pilot Native Identity` 是一次明确派发的只读探测，仅允许原仓库 main 的首次执行，
没有账户、服务、token 或操作覆盖参数。它复用 metadata 入口已使用的原 Variable／Secret
来源、具名 workflow secrets 掩码和固定云身份 bootstrap，不消费独立 daily target。

程序先读取服务、唯一 100% serving revision 和实际 revision 配置。六项身份必须与原受保护
来源一致，实际 revision 的 `runtime_risk_limits.binding.account_hash` 也必须存在且与来源
相同。再次读取服务确认未切换后，才将**实际 revision 中的账户绑定**交给原只读 native
identity 函数；成功后再读一次服务确认仍为同一 revision。服务漂移不能被原账户可访问
这一事实掩盖。

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
