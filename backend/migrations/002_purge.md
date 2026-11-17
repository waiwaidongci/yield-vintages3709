# 按底稿编号清退（002）

已有数据库升级需先备份。本次新增两张表并给三张表加封锁标记列：

- `purge_jobs`：清退任务。`source_ids` 为按编号提交的底稿集合；`scope` 记录本次涉及的
  报告/交付/计划编号；`status` 取 `pending/running/completed/failed`，`step` 记录
  `created→blocked→sources→runs→deliveries→schedules→verify`；`error` 保存失败原因；
  `created/updated/completed` 用于超时接管与页面时间线。
- `source_fingerprints`：底稿每个内容版本的规范化 SHA256（`source_id+fingerprint` 唯一）。
  用于识别旧报告快照里冻结的副本，也用于底稿被普通删除后仍能按编号清退。
- `yield_runs.purged`、`delivery_schedules.purged`、`deliveries.purged`：布尔封锁标记。
  置位后所有读取/下载/复核/计划入口一律拒绝，副作用副本（`snapshot/result/checksum/payload`）
  被清空，`status` 置为 `purged`。行本身保留为墓碑，便于旧入口稳定返回 410 而不是 404。

本次起 `submit` 在 `snapshot.source_refs` 记录精确底稿编号；此前的快照没有该字段，
清退时以 `source_fingerprints` 的内容哈希匹配，避免把同内容的无关报告误删。
`purged` 列默认 `false`，可用如下 SQL 补列（MySQL 8）：

```sql
ALTER TABLE yield_runs ADD COLUMN purged TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE delivery_schedules ADD COLUMN purged TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN purged TINYINT(1) NOT NULL DEFAULT 0;
```

新表由 SQLAlchemy 启动建表；生产环境建议改为显式迁移执行上述语句。
