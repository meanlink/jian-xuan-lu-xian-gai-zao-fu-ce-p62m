# 拣选路线改造复测

配对实验分析的真实业务分析小型项目。完整模拟客户、痛点、上下游、算法公式与一天工作剧本见 [BUSINESS.md](BUSINESS.md)。

## 模块与业务流程
数据接入（JSON/CSV、字段验证、批次指纹）→ 参数化分析（异步 Python worker）→ 结果复核（指标、图形、明细、解释、批准）→ 定时分发（本地收件箱、CSV/JSON、SHA256）；另有业务总览和审计轨迹。
三个贯穿全栈的实体：Dataset 数据批次、Job 分析任务、Report 分发计划；辅助 User、Audit、Delivery。

## 快速启动
```bash
cp .env.example .env
docker compose up -d --build --wait
docker compose exec -T backend python -m scripts.seed_demo
```
访问 http://localhost:18205；后端文档 http://localhost:19205/docs；数据库宿主端口 17705。
`SEED_DEMO=true` 自动幂等 seed；不覆盖用户修改。手动 seed 命令可重复执行。

## 演示账号与权限
|账号|密码|真实权限|
|---|---|---|
|admin@demo.local|Demo123!|接入、执行、批准、分发、暂停、读取与下载|
|analyst@demo.local|Demo123!|接入、执行、读取与下载；不能批准或安排分发|
|viewer@demo.local|Demo123!|只读与下载|

## 技术与本地开发
Python 3.12 / FastAPI / SQLAlchemy / NumPy / SciPy / scikit-learn；MySQL 8.0；Redis 7；Nginx；原生 HTML CSS JavaScript ES modules（用户指定）。后端所有校验、持久化和算法在服务端。无前端打包步骤。
后端：安装 backend/requirements.txt，设置 DATABASE_URL、REDIS_URL、JWT_SECRET、ARTIFACT_DIR 后在 backend/ 执行 `python -m migrations.initial`、`python -m scripts.seed_demo`、`uvicorn src.main:app`，另开 `python -m src.worker`。前端使用带 /api 反向代理的 Nginx，直接打开 index.html 不支持 API。

## 目录与工程约定
backend/src/{models,schemas,services,routes,controllers,middlewares,constants,config,utils}；backend/scripts/seed_demo.py；backend/migrations/initial.py；backend/tests/test_leases.py（租约并发/接管/分发幂等检查）。
frontend/src/{api,stores,types,components/common,hooks,pages,router,utils,constants}。
Role 枚举：admin/analyst/viewer；JobStatus：queued/running/completed/failed。后端 backend/src/constants/enums.py，前端 frontend/src/constants/enums.js；路由权限在 middlewares/auth.py；页面按相同角色控制操作入口。

## 环境变量与 Docker
COMPOSE_PROJECT_NAME=pick-paired；FRONTEND_PORT=18205；BACKEND_PORT=19205；DB_PORT=17705；DB_PASSWORD、JWT_SECRET 见 .env.example；SEED_DEMO 默认 true；JOB_LEASE_SECONDS（默认 20）与 JOB_LEASE_RENEW_SECONDS（默认 5）控制分析租约时长与续期间隔。共享命名产物卷供后端下载与 worker 写入，数据库/Redis 各持久卷。
恰好五服务均有健康检查。Redis 队列加 SQL 排队状态兜底；worker 按 next_run 生成本地收件箱文件。周期 60 秒至 30 天。分发结果为批准分析的固定快照；若需新数据重新计算并建立新计划。当前不连接邮件或企业聊天。

### 并发执行与恢复
分析任务由数据库租约决定执行者，Redis 队列只作为唤醒提示：

- 领取：worker 以条件更新原子地领取排队任务或接管已过期的运行任务，每次领取递增栅栏令牌（fence）；同一任务同一时刻只有一个有效执行者。
- 续期：执行期间后台线程每 `JOB_LEASE_RENEW_SECONDS` 续租；只要进程存活，耗时计算不会被抢走。
- 接管：进程中断后租约到期，其他 worker 自动接管并记录接管事件；旧执行者的迟到成功或失败因令牌不匹配被拒绝，记入轨迹且不覆盖接管结果。
- 队列丢失或重复投递：worker 每轮直接从 SQL 扫描待办，重复消息只会命中已被领取/完成的任务而被跳过。
- 分发幂等：到期计划在同一事务内先抢占 next_run，再写归档；重试不会重复发布文件。
- 结果页“执行与领取轨迹”展示领取、续期、接管、完成/失败与被拒绝的旧写回；已有待办继续可接，已完成与已批准结论保持原值。

worker 可水平扩容（`docker compose up -d --scale worker=N`）；容器健康检查以 `/tmp/heartbeat` 为存活信号，心跳随续租刷新。
错误响应、请求编号、Redis 每 IP 每分钟 240 次限流、JWT 和数据库操作审计。演示密钥与密码仅供本机模拟；生产部署前配置独立密钥与账号。

## 数据与验收
所有数据为固定随机种子生成的模拟业务数据，不能作为实际生产决策证据。项目独立算法、输入边界及业务局限详见 BUSINESS.md。测试与截图统一存放 ../../test-artifacts/gb-1006/，源码不含日志/截图/测试报告。
清理本项目演示数据：`docker compose down -v --remove-orphans`（会删除本项目命名卷）。

## License
MIT，见 LICENSE。
