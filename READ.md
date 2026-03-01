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
- **Python 环境**：流程通过 `conda run -n gatk` 运行；GATK（含 NVScoreVariants）所需的 Python 依赖（PyTorch、scorevariants）已安装在 conda 环境 `gatk` 中，无需改脚本即可使用。

## 从本地 OpenClaw 源码启动网关

钉钉触发生信时会请求本机 `http://127.0.0.1:18789`，需先在本机用**本地 openclaw 源码**把网关跑起来。

**根据本机执行历史（`~/.zsh_history`）确认，之前使用的启动方式为：**

- 网关是 **Node 应用**，不依赖 Python 虚拟环境；终端里是否激活 `.venv_dingtalk_bridge` 对网关无影响，只要当前 shell 的 **Node 版本 ≥ 22.12.0** 即可（openclaw 的 `package.json` 要求）。
- 若用 nvm，先切到 Node 22 再启动（本机已安装 v22.22.0）：

```bash
cd /Users/work/000code/github/openclaw
nvm use 22
pnpm openclaw gateway --port 18789 --verbose
```

即：在 openclaw 源码目录下，**先 `nvm use 22`，再** 执行 **`pnpm openclaw gateway --port 18789 --verbose`**（需已执行过 `pnpm install` 与 `pnpm build`）。

---

其他可选方式：

- **带监听源码变更**：`pnpm run gateway:watch`（改代码自动重启）
- **仅开发模式**：`pnpm run gateway:dev`
- **后台运行**：`nohup pnpm openclaw gateway --port 18789 --verbose >> ../gateway.log 2>&1 &`

---

## 钉钉桥接服务（DingTalk Stream Bridge）

- `run_dingtalk_stream_bridge.sh` 在 conda 环境 `gatk` 下启动钉钉机器人桥接，接收钉钉消息并触发生信流程。
- 日志输出到 `dingtalk_runtime/bridge.log`。
- **前置**：先按上面「从本地 OpenClaw 源码启动网关」把网关启起来，再启钉钉桥接。

**后台启动钉钉桥接：**

```bash
cd /Users/work/000code/github && nohup ./run_dingtalk_stream_bridge.sh >> dingtalk_runtime/bridge.log 2>&1 &
```

## 简化理解

`run_bio_payload.sh`（生成+触发） -> `run_payload.sh`（通用发送） -> 产出 `bio_payload.json`（请求）和 `bio_tools_invoke_result.json`（结果）。