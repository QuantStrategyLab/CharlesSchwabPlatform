# Schwab 不可变执行 outcome

Schwab 在执行前保留原子 claim marker 作为唯一的重复下单拦截依据。执行周期返回后，平台调用 QPK 的 `record_outcome`，在独立的 `execution_outcomes/` 路径以仅创建方式记录终态；它不会覆盖 claim marker。

因此 Cloud Run 运行账户只需读取和创建对象权限。不要为覆盖 marker 赋予广泛的删除或对象管理员权限。已有 outcome 时记录操作会安全返回，claim 继续阻止同一信号重复提交。
