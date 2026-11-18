# 指标契约换版下的历史宽表重算与语义并行

面向学习服务指标的版本化契约、DWS 宽表计算和 ADS 查询基线。

仓库内的实现使用 Python 3.11 标准库和 SQLite，数据只写入命令行指定的本地数据库及导出目录。`lakeapp.storage` 保存原始接收、业务状态、定义注册表和版本清单；`lakeapp.contract` 负责输入约定；`lakeapp.ingest` 接收来源批次并维护期间覆盖度；`lakeapp.derive` 生成当前基线支持的事实与指标宽表；`lakeapp.semantic` 负责指标定义的登记、激活与生效范围；`lakeapp.publish` 读取并写出可追溯版本；`lakeapp.query` 提供期间报告与血缘；`lakeapp.cli` 提供本地操作入口。

## 运行

在 Python 3.11 环境中执行 `python3 -m unittest discover -s tests -v` 运行测试，执行 `python3 -m compileall -q lakeapp tests` 检查构建和语法。示例数据是本地合成记录，不依赖外部服务或联网。

## 现有契约

输入批次包含来源名、批次标识、游标和记录数组。原始记录以 JSON 保存，重复批次不会再次产生事实；业务键相同但内容不同的记录会保留冲突信息。事实查询按业务日期和版本清单读取，清单中的文件摘要与行数用于发现导出内容被替换。数据库初始化由运行入口完成，已有数据不会因重新打开而丢失。

## 指标版本语义

以“有效学习时长”为例，指标定义按版本登记（`define`），初始为草稿；草稿不参与任何计算，`activate` 后才生效。每个版本记录生效范围 `[effective_from, effective_until)`（月度期间），激活新版本时旧版本自动收尾，登记、激活、收尾全部写入 `definition_history`，可用 `definitions` 查询追溯。新版本的生效起点不得早于已激活版本，防止倒签改写历史口径。

新到事件按发生期间适用的已激活定义计入 `metric_facts`，每条事实记录所用定义版本（`metric_version`）。`page_open` 口径计页面打开时长，`active_segments` 口径只计连续互动片段（默认取记录的 `active_value` 字段）。事件缺少所需字段或该期间无生效定义时，事实标记为不完整并保留原因，等待补齐。

`recompute` 对指定历史范围从 ODS 重放，按各期间相应版本的定义重算；已被冻结清单覆盖的月份跳过不改写，原始事件缺失的月份保留现有事实并标注 `raw_missing`。

`report` 按期间给出三态结果：`frozen`（来自冻结清单的最终结果，附定义版本与清单名）、`current`（当前可用，数据完整且口径为当期适用版本）、`pending`（等待补齐，只给 `partial_value` 与原因，绝不给出貌似最终的 `value`）。原因包括 `raw_missing`（原始事件缺失）、`no_active_definition`（无生效定义）、`stale_version`（下游事实尚未按当前定义重算）、`facts_incomplete`（存在缺字段事件）。

`publish-report` 把期间范围快照为 ADS 版本；任何期间不完整时整版为 `incomplete`，`freeze` 仅接受数据完整的 `ready` 版本，冻结单向不可逆，已发布版本不可改写。`lineage` 从 ODS 批次、DWD 事实、DWS 计入值到 ADS 版本追踪一条事实在各层使用的定义版本。

## 目录职责

`lakeapp/storage.py` 负责事务和表结构，`lakeapp/contract.py` 负责记录约束，`lakeapp/ingest.py` 负责接收、来源进度和期间覆盖度，`lakeapp/derive.py` 负责从原始记录生成事实与指标宽表、历史重算，`lakeapp/semantic.py` 负责指标定义注册表与口径计算，`lakeapp/publish.py` 负责版本清单、冻结与回读，`lakeapp/query.py` 负责期间报告与血缘查询，`lakeapp/cli.py` 负责把这些业务操作组合给本地使用者。测试数据覆盖正常接收、重复批次、版本读取、定义换版、历史重算、冻结保护与血缘追踪。
