# database-connection-pool

Python 数据库连接池库，处理事务占用、健康检查、故障切换和公平等待。

连接的创建与查询执行通过注入接口完成（`dbpool.interfaces.ConnectionFactory` /
`Connection`），池本身不依赖任何具体数据库驱动。

## 快速开始

```python
from dbpool import ConnectionPool

pool = ConnectionPool(my_factory, max_size=10, max_per_client=3)

# 普通查询
with pool.acquire(timeout=1.0, client_id="api-worker-1") as conn:
    conn.execute("select 1")

# 事务：整个事务固定同一条连接，提交/回滚后自动归还
with pool.transaction(timeout=1.0) as tx:
    tx.execute("insert into t values (1)")
    tx.execute("update t set v = 2")
```

## 池大小规则

- `max_size`（默认 10）：池中连接总数（空闲 + 借出）的硬上限，连接按需懒创建。
- `max_per_client`（默认 None，不限制）：同一 `client_id` 同时持有的连接数上限，
  超过立即抛出 `ClientLimitError`，避免同一客户端长期占满池。
- 池满时，`acquire(timeout=...)` 进入 FIFO 等待队列，按各请求自己的时限等待；
  超时抛出 `AcquireTimeoutError`，过期请求会被移出队列，不会事后抢到连接。
  只有队首等待者能获得空闲连接或新建名额，保证先到先服务。
- 新建连接在锁外进行，慢 `connect()` 不会阻塞其他借出/归还。
- 任何一条连接同一时刻只会借给一个调用方；`release()` 幂等，
  重复归还不会把连接放回池中两次。`detect_leaks(max_age)` 可检测借出过久的连接。

## 事务规则

- `pool.transaction()` 在事务开始时固定（pin）一条连接，事务内所有语句走同一连接。
- 正常退出时 `commit()` 后归还；抛异常时 `rollback()` 后归还。
- `commit()` 失败时连接状态不可信，直接关闭而不是回收。

## 健康检查规则

- `health_check()` 只探活空闲连接；借出中的连接由调用方负责。
- 每次 ping 失败会按 `health_check_attempts`（默认 3）次重试，
  间隔 `health_check_retry_delay`（默认 0.05s），全部失败才丢弃连接，
  避免瞬时抖动把仍可恢复的连接误判关掉。
- 被丢弃的连接会关闭并腾出池名额，后续获取时新建。

## Failover 规则（generation）

- 池维护单调递增的 `generation`（epoch）。主库切换时：
  1. 把注入的 factory 指向新主库（由调用方/注入方负责切换端点）；
  2. 调用 `pool.failover()`（等价于 `bump_generation()`）。
- bump 之后：
  - 空闲的旧 epoch 连接立即关闭；
  - 借出中的旧 epoch 连接执行新查询会抛出 `StaleConnectionError`，
    归还时直接关闭而不是回收；
  - 新获取的连接由（已指向新主库的）factory 创建，请求不会继续发往旧的只读副本。

## 运行测试

```bash
python -m unittest
```
