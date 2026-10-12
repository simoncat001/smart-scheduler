# Smart Scheduler Backend

`smart_scheduler/backend` 是调度系统的 Python 后端，负责：

- 调度创建、触发、暂停、恢复
- 实验执行记录与反馈闭环
- 边端上传数据汇聚
- `mat_model` 推荐结果接入
- 在 `../ui/dist` 存在时托管构建后的 Vue 前端

## 目录

- `app/`：FastAPI、模型、服务层、XRD 解析
- `mat_model/`：调度内置模型逻辑
- `tests/`：后端测试
- `../ui/`：Vite + Vue 前端工程
- `../model_assets/`：模型配置与离线产物
- `../四元数据/`：XRD 数据目录

## 启动

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/backend
export PYTHONPYCACHEPREFIX="$HOME/.local/share/smart_scheduler/cache/python"
export MPLCONFIGDIR="$HOME/.local/share/smart_scheduler/cache/matplotlib"
/opt/anaconda3/envs/dev/bin/python main.py
```

正式运行使用本地 MySQL，请先在后端 `.env` 中配置
`SMART_CHEDULER_DATABASE_URL`，例如下面的占位格式（替换为本地配置）：

```dotenv
SMART_CHEDULER_DATABASE_URL=mysql+pymysql://USER:PASSWORD@127.0.0.1:3306/smart_scheduler?charset=utf8mb4
```

新流程初始化会拒绝 SQLite。旧服务保留的 SQLite 回退仅用于兼容测试，
不应作为此项目的运行数据库。

如果你还想保留热重载，也可以：

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/backend
export PYTHONPYCACHEPREFIX="$HOME/.local/share/smart_scheduler/cache/python"
export MPLCONFIGDIR="$HOME/.local/share/smart_scheduler/cache/matplotlib"
SMART_CHEDULER_RELOAD=1 /opt/anaconda3/envs/dev/bin/python main.py
```

## 前端开发

开发模式：

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/ui
npm install
npm run dev
```

访问：

- [http://127.0.0.1:5174](http://127.0.0.1:5174)

构建并交给 FastAPI 托管：

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/ui
npm run build
```

构建后访问：

- [http://127.0.0.1:8001](http://127.0.0.1:8001)

## 测试

```bash
cd /Users/liye/Code/feed_back_sys/smart_scheduler/backend
export PYTHONPYCACHEPREFIX="$HOME/.local/share/smart_scheduler/cache/python"
export MPLCONFIGDIR="$HOME/.local/share/smart_scheduler/cache/matplotlib"
/opt/anaconda3/envs/dev/bin/python -m pytest -o cache_dir="$HOME/.local/share/smart_scheduler/cache/pytest"
```

本机旧 `.venv` 是不可用的 Linux 环境，已移到项目外归档。
依赖与产物迁移方式、新建外部虚拟环境的方法见[项目说明](../README.md)。

## 可配置流程改造

节点定义、版本管理，以及第二阶段的轻量流程调度和 Kafka 收发已实现，
不再要求设备资源登记，也不管理设备占用。第三阶段已接入现有调度触发、
暂停与恢复接口，并隔离旧反馈/推理路径。第四阶段已增加流程草稿/发布 API、
节点级 HTTP 上传、幂等结果关联和操作审计。模型/解析器/模拟器执行端已实现
持久化任务、取消去重、独立计算循环和固定版本结果回传。第七阶段已接入独立 Vue Flow 编排页和节点实例运行监控，
正式设备执行前仍需完成真实消息与数据库联调。
初始化步骤和当前边界见[流程基础层说明](/Users/liye/Code/feed_back_sys/smart_scheduler/docs/workflow-foundation.md)。
新增流程使用本地 MySQL，初始化命令不会启用设备或触发实验。
独立 worker 启动方式和消息约定见[Kafka 流程说明](../docs/kafka-workflows.md)。
流程绑定、运行状态接口和外部调度模式见[调度接入说明](../docs/workflow-scheduling.md)。
流程管理、节点上传、审批/取消/计算重试及当前健康状态边界见[流程 API 说明](../docs/workflow-api.md)。
固定模型版本、各 worker 启动方式和取消/恢复边界见[执行端说明](../docs/workflow-executors.md)。

## 常用环境变量

- `SMART_CHEDULER_DATABASE_URL`
- `SMART_CHEDULER_SCHEDULER_ENABLED`
- `SMART_CHEDULER_SCHEDULER_MODE` (`inline` / `external`)
- `SMART_CHEDULER_MANAGE_DB_BASE_URL`
- `SMART_CHEDULER_MANAGE_DB_USERNAME`
- `SMART_CHEDULER_MANAGE_DB_PASSWORD`
- `SMART_CHEDULER_MANAGE_DB_TIMEOUT_SECONDS`
- `SMART_CHEDULER_MAT_MODEL_ONLINE_REFRESH_ENABLED`
- `SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_ENABLED`
- `SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_DATA_CATEGORY`
- `SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_TEMPLATE_TYPE`
- `SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_PAGE_SIZE`
- `SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_MAX_RECORDS`
- `SMART_CHEDULER_XRD_DATA_DIR`
- `SMART_CHEDULER_MAT_MODEL_CONFIG_PATH`
- `SMART_CHEDULER_MAT_MODEL_OUTPUT_DIR`

## 配置边界

`smart_scheduler/backend` 只读取自己的 [`.env`](/Users/liye/Code/feed_back_sys/smart_scheduler/backend/.env) 和 `SMART_CHEDULER_*` 环境变量。

- 不会读取 `remote_trans/config/config.json`
- 不会复用 `manage_db` 的 `.env`
- `manage_db` 凭据必须在 `smart_scheduler/backend/.env` 里单独配置

## 闭环链路

当前调度闭环已经按这条链路工作：

1. 调度派发本轮执行，生成 `experiment_batch_id` 和本轮 `sample_id`
2. 边端先请求：
   - `GET /api/v1/executions/{execution_id}/edge-context`
   - `GET /api/v1/executions/{execution_id}/sample-plan`
3. `remote_trans` 在上传 `manage_db` 前，把本轮 `sample_id` 写入待上传元数据
4. 上传完成后，边端回调：
   - `POST /api/v1/closed-loop/data-packages`
5. 调度库保存 `manage_db_record_id`
6. 当闭环条件满足后，`mat_model` 会优先按这些已回写的 `manage_db_record_id` 精确回取研发数据，再做下一轮推理刷新
