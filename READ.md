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

---

## 钉钉报错排查（根因与修复）

**现象**：钉钉触发生信后收到「报错」，网关返回 HTTP 500，`error.message` 为 `"tool execution failed"`。

**已确认根因（2026-03-01 10:57 会话）**：

1. **网关侧 lobster 子进程未设置 `GFORTRAN`**  
   钉钉桥接通过 `run_bio_payload.sh` → `run_payload.sh` → `curl` 调用网关；实际执行 `conda run -n gatk python3 ... run_bioinformatics_analysis.py` 的是**网关进程**启动的 lobster 子进程。网关环境里通常没有设置 `GFORTRAN`。

2. **gatk 环境的 conda activate 脚本依赖 `GFORTRAN`**  
   `dingtalk_runtime/bridge.log` 中曾出现：`GFORTRAN: unbound variable`（来自 `miniconda3/envs/gatk/.../activate-gfortran_osx-64.sh`）。在启用 `set -u` 或类似严格模式下，未设置 `GFORTRAN` 会导致 activate 报错并退出。

3. **结果**：lobster 子进程非零退出 → 网关返回 500，客户端只看到 `"tool execution failed"`。

**已做修复**：

- **run_bio_payload.sh**：在生成的 pipeline 中，在 `conda run -n gatk` 前增加 `export GFORTRAN="${GFORTRAN:-}";`，使网关侧执行的 shell 在激活 gatk 前就设定 `GFORTRAN`，避免 unbound variable。
- **网关**（openclaw）：500 时在响应体中返回真实错误信息（如 `lobster failed (code 1): ...`），便于后续从钉钉或 `*_result.json` 直接看到原因。
- **钉钉桥接**：流程失败时解析 `*_result.json` 的 `error.message` 并推送到钉钉，展示「网关错误: xxx」。

**若再次报错**：查看钉钉消息中的「网关错误」或 `dingtalk_runtime/*_result.json` 的 `error.message`；若网关以前台运行，可同时查看终端里的 `tools-invoke: tool execution failed: ...` 日志。

---

## 钉钉发了但没收到进度/结果推送

**现象**：在钉钉发了「重新帮我运行」等触发语，但没有任何进度或结果消息。

**排查**：

1. **桥接是否收到消息**  
   - 看 `dingtalk_runtime/` 下是否有**该时间点**的 `incoming_YYYYMMDD_HHMMSS_*.json`（以及同时间戳的 `*_payload.json` / `*_result.json`）。  
   - 若**没有**对应时间点的文件 → 桥接**没收到**这条消息（桥接未跑、或 Stream 断连）。  
   - 重启桥接并**把标准输出重定向到日志**，便于后续查：  
     `nohup ./run_dingtalk_stream_bridge.sh >> dingtalk_runtime/bridge.log 2>&1 &`  
   - 之后可看 `dingtalk_runtime/last_message_received.log` 内容是否在更新（每次收到消息会覆盖写入最后一条时间）。

2. **进度是在“等网关”期间推送的**  
   桥接在调用网关（curl）后，会轮询 `test_data/out_dingtalk/` 下的进度文件并推送到钉钉。若网关很快返回 500 或未启动，则几乎没有进度可推，只会有一条失败或异常推送。

3. **Stream 断连**  
   若日志里出现 `keepalive ping timeout`、`timed out during opening handshake`，说明与钉钉长连接断开。桥接会自动重连，但断连期间的消息可能收不到，可重启桥接再发一次。