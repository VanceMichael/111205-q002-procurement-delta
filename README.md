# 采购公告更正与增量游标

## 现有能力

- 来源域与公告号的复合身份（`service.notice_key`）：不同平台的相同公告号互不冲突。
- 可切换 `full` / `incremental` 两种模式的重放工作流（`collector.py`），按轮次应用本地记录的分页响应。
- 复合游标，不仅靠最后一条时间戳推进：
  - `high_water`：最高发布时间 + 该时刻已见的全部公告键，并列发布时间跨午夜重启后既不漏抓也不重复；
  - `scan`：进行中轮次的标识、下一页序号与已提交页哈希，页面分页漂移时按公告身份去重，可从任意页失败后继续；
  - `corrections`：更正公告 → 原公告的关联，可双向查询，原文摘要随审计日志保留；
  - `mode`：最近一次使用的扫描模式。
- 幂等重放：已提交的页按内容哈希跳过，下游 outbox 按事件 id 去重，重放相同批次无副作用。
- 安全淘汰：只有标记完整（complete）的全量（full）扫描结束后，才把当前索引中消失的记录标记为 `retired`；审计日志只追加、永不清除，增量扫描永不淘汰。

`service.py` 保持已有规范化接口。根目录 `test_service.py` 为原有回归测试，`tests/test_baseline.py` 会同时加载原测试和离线演示验证，`tests/test_collector.py` 覆盖并列时间、跨页漂移、更正关联、断点续跑与淘汰规则等交错情形。

## 环境和运行

使用 Python 3.11.10，仅依赖标准库，无需登录账号、安装第三方包或下载数据。`.python-version` 固定解释器版本；`pyproject.toml` 仅登记 unittest 入口，项目不需要打包安装。

在仓库根目录执行：

```sh
python3 -m unittest discover -s tests -v
python3 demo.py
```

完整演示命令：

```sh
python3 -m unittest discover -s tests -v && python3 demo.py
```

演示实时读取合成输入并运行当前接口，通常在 90 秒以内结束，不回放历史日志。状态写入临时目录，重复执行不污染仓库；本地 HTTP 示例只访问回环地址，不连接生产系统。

## 命令行工作流

状态写入指定的 JSON 文件（每提交一页原子落盘），可反复重放：

```sh
python3 collector.py replay --state /tmp/collector-state.json --mode full
python3 collector.py replay --state /tmp/collector-state.json --mode incremental
python3 collector.py status --state /tmp/collector-state.json
python3 collector.py lineage --state /tmp/collector-state.json --key a.example:7
```

`replay` 逐页报告差异（`new` / `updated` / `corrections` / `retired`）与当时游标，可逐条核对。注入故障验证断点续跑：

```sh
python3 collector.py replay --state /tmp/collector-state.json --fail-at drift:1   # 退出码 3
python3 collector.py replay --state /tmp/collector-state.json --mode full         # 从断点页继续
```

## 合成来源

`fixtures/scans.json` 包含三轮分页响应；每轮独立标识是否完整，覆盖更正、页面漂移与索引消失样本。

## 能力边界

- 仅重放本地记录的分页响应，不直连平台抓取；接入真实平台时需为每次扫描分配新的轮次标识。
- 状态为单文件 JSON，页提交后原子落盘；不覆盖多机并发写入。
- 同秒并列公告键全量保存在游标 `ties` 中，数量极大时需要额外的压缩策略。
