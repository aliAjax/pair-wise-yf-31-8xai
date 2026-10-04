# 航班中断恢复系统

独立的 Python 标准库项目，用 SQLite 保存机场、飞机、机组、航线许可、航班、中断事件和恢复方案。系统会校验维护间隔、执勤时限、机场宵禁、航线许可、资源重叠，并计算取消、延误、受影响旅客和错失衔接成本。

## 运行

```bash
python3 app.py --db airline_recovery.db
```

默认监听 `127.0.0.1:8202`，首页为 `/`，健康检查为 `/health`。

身份头：`X-User-Id`、`X-Role`。角色包括 `viewer`、`scheduler`、`ops_manager`、`auditor`。

## 主要接口

- `POST /api/airports`、`/api/aircraft`、`/api/crew`、`/api/permits`：基础资源与约束。
- `POST /api/flights`、`POST /api/disruptions`：创建航班和中断。
- `POST /api/recovery-plans`：一次提交方案及航班调整。
- `POST /api/plans/{id}/assignments`：用 `expected_revision` 临时改派。
- `POST /api/plans/{id}/validate`、`/lock`：校验并原子锁定方案。
- `GET /api/disruptions/{id}/compare`：比较恢复方案成本。
- `POST /api/flights/{id}/cancel`、`/recover`：取消和人工恢复。
- `GET /api/state`、`GET /api/plans/{id}`：查询状态和影响。

## 调机链与时隙

外地抛锚时手工拼调机并接入恢复方案：

- `POST /api/ferries`：提交调机链。`ferry_id` 为客户端指定编号；`plan_id` 可挂到草稿方案；`legs[]` 按顺序给出 `origin/destination/duration_minutes/ground_minutes`，首段可用 `predecessor_kind="flight"` + `predecessor_ref` 串接前任航班（后续航段自动链接，起点必须等于上一段落地）。系统按前任落地机场和落地时间计算最早可行性；为每段的起飞、落地机场在时间窗口上占用一条时隙（`held`）。
- 时隙容量：机场按 `slot_window_minutes`（默认 60）切窗，`slot_capacity`（默认 3）为每窗容量，`slot_horizon_windows`（默认 24）为向前搜索的窗口数。容量不足时航段保持 `queued` 并入 FIFO 队列；任何释放（落地、取消、退回）都会触发队首重算。
- 同一架飞机并发提交：写事务经 `BEGIN IMMEDIATE` + 进程锁串行仲裁，先占到时隙者继续，后到者收到 `409 ferry_aircraft_conflict`。与已锁定方案的航班重叠同样拒绝。
- `POST /api/ferries/{id}`：事件 `depart` / `arrive`（可带 `at`）/ `cancel_leg` / `cancel`，均需 `expected_revision`。航段状态一变，后续未执行航段立即释放占用、退回队列并按新的落地位置和时间重算；落地/取消后腾出的容量只计一次（条件 UPDATE 幂等，同一航段有唯一占用索引）。
- 写入失败重试：用相同 `ferry_id` 与相同载荷重试是幂等的（返回已有调机）；编号被不同内容占用则返回 `409 ferry_id_conflict`。
- 方案锁定 `POST /api/plans/{id}/lock`：方案下若有调机航段仍在排队（`ferry_not_ready/ferry_legs_waiting`）或与其他调机存在同机重叠则拒绝；锁定成功后调机时隙从 `held` 转为 `confirmed`。
- `POST /api/slot-config`：运行经理调整机场窗口分钟数、容量、前瞻窗口数（已建窗口不变，后续新窗口生效）。
- 调度台视图：`GET /api/ferries?plan_id=`（调机链、每段状态和队列位置）、`GET /api/slots?airport=&date=YYYY-MM-DD`（窗口占用/余量、占用明细、等待航段），首页 `/` 图形化展示调机链、等待队列和时隙占用。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

时区和机场本地时刻没有引入完整时区数据库；模型使用简化航线许可与宵禁规则。身份头、SQLite 和单进程 HTTP 服务适合原型演示，正式运行需要外部身份系统、共享数据库和更强的跨实例锁。
