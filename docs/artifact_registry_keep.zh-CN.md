# Schwab 发布 KEEP 与两级回滚来源

普通 image deploy 和 notification-only native publish 在创建 Cloud Run revision 之前，调用 `scripts/refresh_artifact_registry_keep.py`。helper 读取当前区域的 Cloud Run 服务、Jobs、未完成 executions，合并正流量、零流量 tag、latest ready/created、candidate 和指定 R1/R2 的 digest，只向既有 KEEP 规则追加必要前缀，保留其他五条规则及原有保护。更新失败或读回不一致会停止发布。

helper 只接受 `cleanupPolicyDryRun=true`，更新命令显式带 `--dry-run`，并在更新前后检查模式；没有启用真实镜像清理的入口。原 workflow 的镜像直删已经移除，镜像清理由现有 Artifact Registry policies 管理。Cloud Run revision prune 与 Artifact Registry dry-run 是两套机制：前者仍受原有 `cleanup_enabled` 和 `approve_cleanup` 条件约束，允许删除不在保留集内的 revision。它保留正流量、所有 tag、latest ready/created 和 R1/R2，库存不完整、服务漂移或首次删除失败即停止。

## 待 owner 提供的来源

`SCHWAB_ARTIFACT_REGISTRY_ROLLBACK_MANIFEST_JSON` 是非秘密 JSON 输入接口，来自调用仓库可用的 GitHub variable。workflow 以 step env 注入，不将 JSON 拼入 shell 命令、命令参数或输出。普通 variable 本身没有受保护存储/Environment 审批语义，`source` 字段也只记录出处，不证明出处真实或审批有效。

配置该接口前，release owner 需要提供实际指定及验收记录、现有受保护存储的准确资源名/版本，以及维护/审批入口。应确认非秘密投影如何从受保护来源产生、谁可修改以及如何审阅；如果需要直接读取既有受保护来源，必须另外实现准确资源的读取接口。空输入、占位符、短 digest、重复 digest/revision、缺服务或 image package 不符均报 `NEED_USER` 并停止，不从现有 KEEP 或最新镜像推断 R1/R2。

以下是**不可执行的占位契约**，使用前需要替换所有占位字段：

```json
{
  "schema_version": 1,
  "source": {
    "resource": "<existing protected source resource>",
    "version": "<immutable source version>"
  },
  "services": {
    "<exact service name>": {
      "image_repo": "<region>-docker.pkg.dev/<project>/<repository>/<exact image package>",
      "rollbacks": [
        {
          "level": "R1",
          "revision": "<designated revision>",
          "digest": "<full sha256 digest>",
          "release": "<release identity>",
          "designated_by": "<release owner>",
          "acceptance_evidence": "<designation and acceptance record>",
          "retirement_condition": "<retention deadline or explicit retirement condition>"
        },
        {
          "level": "R2",
          "revision": "<different designated revision>",
          "digest": "<different full sha256 digest>",
          "release": "<release identity>",
          "designated_by": "<release owner>",
          "acceptance_evidence": "<designation and acceptance record>",
          "retirement_condition": "<retention deadline or explicit retirement condition>"
        }
      ]
    }
  }
}
```

每个发布服务单独映射；选中服务必须有两个不同的完整 digest 和 revision。helper 实时读取两个 revision，验证服务归属、精确 image package 和 resolved digest，再读取对应 Artifact Registry image，确认 digest 存在。`acceptance_evidence` 和退役条件由 owner 负责，helper 不执行业务回滚资格验收，也不自动按日期解除保护。其他服务的条目不会被当作选中服务的 R1/R2；当前仓库其他运行引用仍由区域库存保护。

## 发布前提与边界

- 两条发布路径仍要求原有显式批准，并保留 no-traffic 和 notification baseline 限制。
- KEEP helper 在实际发布时会更新 repository 配置；发布 SA 需要精确仓库级配置更新权限，IAM allow 读回不能替代实际更新及读回验收。
- 必须由原发布 owner 确认同窗口没有其他 ref 或外部 writer。现有 workflow/ref 并发组不构成跨 ref、管理员和全区域的互斥锁，fresh read 也不是原子事务。
- 区域库存限定为配置的 project/region/repository；跨区域引用、受保护来源接线及真实清理切换需要另行验收。
- 保留的前缀只增不减，可能继续保留已退役镜像；helper 不实现解除保护。

离线验证：`python3 -m pytest -q tests/test_refresh_artifact_registry_keep.py tests/test_schwab_keep_release_contract.py`、既有两个 workflow shell tests，以及 `actionlint .github/workflows/sync-cloud-run-env.yml`。

参考：[Artifact Registry cleanup policies](https://docs.cloud.google.com/artifact-registry/docs/repositories/cleanup-policy)、[cleanup policy overview](https://docs.cloud.google.com/artifact-registry/docs/repositories/cleanup-policy-overview)。
