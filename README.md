# Smart Scheduler

调度系统已经拆成前后端双目录：

- [backend/README.md](/Users/liye/Code/feed_back_sys/smart_scheduler/backend/README.md)：FastAPI 后端、测试、`mat_model`
- `ui/`：Vite + Vue 前端工程
- `model_assets/`：模型配置与离线产物
- `四元数据/`：XRD 数据目录

## 本地开发

后端：

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/backend
export PYTHONPYCACHEPREFIX="$HOME/.local/share/smart_scheduler/cache/python"
export MPLCONFIGDIR="$HOME/.local/share/smart_scheduler/cache/matplotlib"
/opt/anaconda3/envs/dev/bin/python main.py
```

前端：

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/ui
npm install
npm run dev
```

开发访问地址：

- 后端 API：[http://127.0.0.1:8001](http://127.0.0.1:8001)
- 前端 Vite：[http://127.0.0.1:5174](http://127.0.0.1:5174)

## 构建前端

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/ui
npm run build
```

构建完成后，FastAPI 会直接托管 `ui/dist`，访问：

- [http://127.0.0.1:8001](http://127.0.0.1:8001)

## 流程编排与监控

主页左上角的流程图标打开独立编排页：
[http://127.0.0.1:5174/?view=workflow-builder](http://127.0.0.1:5174/?view=workflow-builder)。
支持节点增删复制、拖拽、控制流与数据端口连接、网关/适配器配置、重试与超时、草稿保存、校验、版本发布和调度绑定。
发布版本只读；编辑草稿不会改动运行中的流程。保存冲突时保留本地编辑，可以导出 JSON 或复制为新草稿。

新增节点的输入/输出端口是 JSON 对象，例如：

```json
{"sample":{"schema":{"type":"object","minProperties":1},"required":true}}
```

材料模板的模型版本必须配置为实际内容固定版本，不能使用 `unconfigured`。
在后端目录执行只读命令获取模型标识，再填入模型节点配置：

```bash
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker model --describe-model
```

绑定仅影响后续执行，不会自动触发设备。三个表征节点并行等待上传；模型节点在收到有效依赖结果之前显示“待预测开始”。
新流程的状态按节点实例映射，额外节点在原高度侧栏内滚动；未绑定执行保留原 D3 图。
消息状态面板展示收发计数和 Worker 心跳，`Kafka 未确认` 不代表已验证真实连接。
节点详情支持人工审批、计算重试和已派发节点的取消请求，不提供设备自动重试或资源占用管理。

前端验证：

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/ui
npm test
npm run build
# 安装了 Playwright 和 Chrome 的独立测试环境：
node tests/browser-workflow.mjs
```

浏览器测试拦截全部 `/api/` 请求，使用隔离数据验证编辑、发布、绑定和运行状态，
不会连接 MySQL、Kafka 或设备；这不能替代下一阶段的真实集成验证。

没有后端时可以运行独立模拟预览：

```bash
npm run dev:workflow-preview
```

[http://127.0.0.1:5175/?view=workflow-builder](http://127.0.0.1:5175/?view=workflow-builder)
使用标注“隔离预览”的测试数据；全部 API 被本地测试服务拦截，保存仅存在内存中，重启即重置。
正常开发仍使用 5174 连接实际后端。桌面主页区域尺寸不变；新流程的移动端单列显示修正了画布遮挡，未修改旧流程移动端布局。

## 本地依赖与产物

本机依赖、缓存、构建产物和模型运行结果实际存放在
`/Users/liye/.local/share/smart_scheduler/`，项目中通过符号链接保持原路径可用。
`ui/node_modules`、`ui/dist`、`runtime` 和模型输出目录的链接应保留；
`npm run build` 会继续写入项目外的构建目录。

旧 `backend/.venv` 来自 Linux，解释器链接在本机失效，已归档。
本机使用已有的 `/opt/anaconda3/envs/dev/bin/python` 环境。
需要新的独立环境时，在项目外创建：

```bash
python3 -m venv "$HOME/.local/share/smart_scheduler/venv"
source "$HOME/.local/share/smart_scheduler/venv/bin/activate"
cd /Users/liye/Code/feed_back_sys/smart_scheduler/backend
python -m pip install -e ".[test]"
```

清理归档和迁移清单见外部目录的 `archives/` 与 `latest-cleanup.json`。
源码、XRD 原始数据、模型配置和数据库文件保留，清理文件均可恢复。
`.ignore` 控制日常搜索范围，Serena 仅索引后端 Python 工作区。
