# 文件说明

这 4 个文件作用如下：

## `bio_payload.json`

- 给 OpenClaw 的请求体模板（`tool=lobster + args.pipeline`）。
- 相当于“这次要执行什么流程”的配置快照。

## `bio_tools_invoke_result.json`

- 调用 `/tools/invoke` 后保存的响应结果。
- 相当于“这次执行的回执/日志输出”。

## `run_payload.sh`

- 通用调用器：读取 token、发送 payload.json、保存结果到文件。
- 它不关心生信内容，任何 payload 都能用。

## `run_bio_payload.sh`

- 生信专用一键脚本：自动生成 `bio_payload.json`，再调用 `run_payload.sh` 执行。
- 日常你只跑这个就行（你现在的一键入口）。

## 简化理解

`run_bio_payload.sh`（生成+触发） -> `run_payload.sh`（通用发送） -> 产出 `bio_payload.json`（请求）和 `bio_tools_invoke_result.json`（结果）。