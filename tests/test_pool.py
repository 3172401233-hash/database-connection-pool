import threading
import time
import unittest

from dbpool import (
    AcquireTimeoutError,
    ClientLimitError,
    ConnectionPool,
    ConnectionReleasedError,
    PoolClosedError,
    StaleConnectionError,
)


class FakeConnection:
    """注入用的假连接：可控制 ping 失败次数，记录执行过的查询。"""

    def __init__(self, name):
        self.name = name
        self.closed = False
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.ping_attempts = 0
        self.transient_ping_failures = 0
        self.ping_always_fails = False
        # 并发借用检测：execute 期间计数不为 1 即说明被同时借给两个调用方
        self.active_executes = 0
        self.concurrent_violations = 0
        self._lock = threading.Lock()

    def execute(self, query):
        if self.closed:
            raise RuntimeError("connection is closed")
        with self._lock:
            self.active_executes += 1
            if self.active_executes != 1:
                self.concurrent_violations += 1
        try:
            time.sleep(0.001)
            self.executed.append(query)
            return f"{self.name}:{query}"
        finally:
            with self._lock:
                self.active_executes -= 1

    def ping(self):
        if self.closed:
            raise RuntimeError("connection is closed")
        self.ping_attempts += 1
        if self.ping_always_fails:
            raise RuntimeError("ping failed")
        if self.transient_ping_failures > 0:
            self.transient_ping_failures -= 1
            raise RuntimeError("transient ping failure")

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FakeFactory:
    """注入用的假工厂：target 模拟当前主库端点，failover 时切换。"""

    def __init__(self, target="primary"):
        self.target = target
        self.created = []
        self._lock = threading.Lock()

    def connect(self):
        with self._lock:
            conn = FakeConnection(f"{self.target}-{len(self.created)}")
            self.created.append(conn)
            return conn


def make_pool(max_size=2, **kwargs):
    factory = FakeFactory()
    pool = ConnectionPool(factory, max_size=max_size, **kwargs)
    return pool, factory


class TestConnectionLeak(unittest.TestCase):
    def test_leak_is_detected_and_release_clears_it(self):
        pool, _ = make_pool()
        handle = pool.acquire(client_id="greedy-client")
        self.assertEqual(pool.stats().checked_out, 1)
        leaks = pool.detect_leaks(max_age=0)
        self.assertEqual(len(leaks), 1)
        self.assertEqual(leaks[0].holder, "greedy-client")
        handle.release()
        self.assertEqual(pool.detect_leaks(max_age=0), [])
        self.assertEqual(pool.stats().checked_out, 0)

    def test_context_manager_releases_connection(self):
        pool, _ = make_pool()
        with pool.acquire() as handle:
            handle.execute("select 1")
        self.assertEqual(pool.stats().checked_out, 0)
        self.assertEqual(pool.stats().idle, 1)

    def test_released_handle_cannot_execute(self):
        pool, _ = make_pool()
        handle = pool.acquire()
        handle.release()
        with self.assertRaises(ConnectionReleasedError):
            handle.execute("select 1")
        # 重复归还不会把连接放回池中两次
        handle.release()
        self.assertEqual(pool.stats().idle, 1)


class TestTransaction(unittest.TestCase):
    def test_transaction_pins_connection_and_returns_after_commit(self):
        pool, factory = make_pool(max_size=1)
        with pool.transaction() as tx:
            tx.execute("insert into t values (1)")
            tx.execute("insert into t values (2)")
            # 事务期间池被占满，其他请求（零超时）拿不到连接
            with self.assertRaises(AcquireTimeoutError):
                pool.acquire(timeout=0)
        conn = factory.created[0]
        self.assertEqual(conn.commits, 1)
        self.assertEqual(conn.rollbacks, 0)
        self.assertEqual(conn.executed, ["insert into t values (1)", "insert into t values (2)"])
        # 提交后连接归还，可复用同一条连接
        self.assertEqual(pool.stats().idle, 1)
        with pool.acquire() as handle:
            self.assertIs(handle.raw, conn)

    def test_transaction_rolls_back_on_error(self):
        pool, factory = make_pool(max_size=1)
        with self.assertRaises(ValueError):
            with pool.transaction() as tx:
                tx.execute("delete from t")
                raise ValueError("boom")
        conn = factory.created[0]
        self.assertEqual(conn.rollbacks, 1)
        self.assertEqual(conn.commits, 0)
        self.assertEqual(pool.stats().idle, 1)

    def test_slow_transaction_blocks_then_waiter_proceeds(self):
        pool, factory = make_pool(max_size=1)
        entered = threading.Event()
        finish = threading.Event()

        def slow_tx():
            with pool.transaction() as tx:
                entered.set()
                finish.wait(2)
                tx.execute("slow update")

        thread = threading.Thread(target=slow_tx)
        thread.start()
        self.assertTrue(entered.wait(2))
        # 短时限请求超时，不会被慢事务无限期拖住
        with self.assertRaises(AcquireTimeoutError):
            pool.acquire(timeout=0.05)
        finish.set()
        thread.join(2)
        # 慢事务提交后连接归还，后续请求正常执行
        with pool.acquire(timeout=1) as handle:
            self.assertEqual(handle.execute("select 1"), "primary-0:select 1")
        self.assertEqual(len(factory.created), 1)


class TestHealthCheck(unittest.TestCase):
    def test_transient_failure_does_not_kill_recoverable_connection(self):
        pool, factory = make_pool(health_check_attempts=3, health_check_retry_delay=0.001)
        handle = pool.acquire()
        handle.release()
        conn = factory.created[0]
        conn.transient_ping_failures = 1  # 第一次 ping 失败，之后恢复
        report = pool.health_check()
        self.assertEqual(report.healthy, 1)
        self.assertEqual(report.dropped, 0)
        self.assertFalse(conn.closed)
        self.assertGreaterEqual(conn.ping_attempts, 2)
        self.assertEqual(pool.stats().total, 1)

    def test_permanently_dead_connection_is_dropped_after_retries(self):
        pool, factory = make_pool(health_check_attempts=3, health_check_retry_delay=0.001)
        handle = pool.acquire()
        handle.release()
        conn = factory.created[0]
        conn.ping_always_fails = True
        report = pool.health_check()
        self.assertEqual(report.dropped, 1)
        self.assertTrue(conn.closed)
        self.assertEqual(conn.ping_attempts, 3)  # 重试满次数才放弃
        self.assertEqual(pool.stats().total, 0)
        # 池中名额被释放，下一次获取会新建连接
        with pool.acquire() as handle2:
            self.assertIsNot(handle2.raw, conn)


class TestFailover(unittest.TestCase):
    def test_old_generation_cannot_execute_new_queries(self):
        pool, factory = make_pool(max_size=1)
        old = pool.acquire()
        old.execute("select before failover")
        # 主库切换：factory 指向新主，池进入新 epoch
        factory.target = "new-primary"
        pool.failover()
        with self.assertRaises(StaleConnectionError):
            old.execute("select after failover")
        # 旧连接归还时被关闭而不是回收
        old.release()
        self.assertTrue(factory.created[0].closed)
        self.assertEqual(pool.stats().total, 0)
        # 新请求走新主库
        with pool.acquire() as handle:
            result = handle.execute("select 1")
        self.assertEqual(result, "new-primary-1:select 1")

    def test_idle_stale_connections_are_closed_on_bump(self):
        pool, factory = make_pool()
        h1 = pool.acquire()
        h2 = pool.acquire()
        h1.release()
        h2.release()
        self.assertEqual(pool.stats().idle, 2)
        pool.bump_generation()
        self.assertEqual(pool.stats().idle, 0)
        self.assertEqual(pool.stats().total, 0)
        self.assertTrue(all(c.closed for c in factory.created))

    def test_stale_idle_connection_is_never_reused(self):
        pool, factory = make_pool(max_size=1)
        handle = pool.acquire()
        handle.release()
        pool.bump_generation()
        with pool.acquire() as handle2:
            self.assertIsNot(handle2.raw, factory.created[0])
            self.assertEqual(handle2.generation, 1)


class TestAcquireTimeoutAndFairness(unittest.TestCase):
    def test_acquire_times_out_when_pool_exhausted(self):
        pool, _ = make_pool(max_size=1)
        pool.acquire()
        start = time.monotonic()
        with self.assertRaises(AcquireTimeoutError):
            pool.acquire(timeout=0.05)
        self.assertGreaterEqual(time.monotonic() - start, 0.05)

    def test_expired_waiter_does_not_steal_connection(self):
        pool, _ = make_pool(max_size=1)
        holder = pool.acquire()
        with self.assertRaises(AcquireTimeoutError):
            pool.acquire(timeout=0.02)
        self.assertEqual(pool.stats().waiters, 0)
        holder.release()
        # 过期等待者已出队，连接可被后续请求正常获取
        with pool.acquire(timeout=0.5) as handle:
            handle.execute("select 1")

    def test_waiters_are_served_in_fifo_order(self):
        pool, _ = make_pool(max_size=1)
        holder = pool.acquire()
        order = []

        def wait_and_use(name):
            with pool.acquire(timeout=2) as handle:
                order.append(name)
                time.sleep(0.05)

        t1 = threading.Thread(target=wait_and_use, args=("first",))
        t2 = threading.Thread(target=wait_and_use, args=("second",))
        t1.start()
        time.sleep(0.1)  # 确保 first 先排队
        t2.start()
        time.sleep(0.1)  # 确保 second 已排队
        holder.release()
        t1.join(2)
        t2.join(2)
        self.assertEqual(order, ["first", "second"])

    def test_client_cannot_monopolize_pool(self):
        pool, _ = make_pool(max_size=3, max_per_client=1)
        hog = pool.acquire(client_id="hog")
        with self.assertRaises(ClientLimitError):
            pool.acquire(client_id="hog", timeout=1)
        # 其他客户端不受影响
        with pool.acquire(client_id="other", timeout=0.5) as handle:
            handle.execute("select 1")
        hog.release()
        # 归还后该客户端可以再次获取
        pool.acquire(client_id="hog").release()


class TestNoDoubleLending(unittest.TestCase):
    def test_distinct_callers_get_distinct_connections(self):
        pool, factory = make_pool(max_size=2)
        h1 = pool.acquire()
        h2 = pool.acquire()
        self.assertIsNot(h1.raw, h2.raw)
        with self.assertRaises(AcquireTimeoutError):
            pool.acquire(timeout=0)
        h1.release()
        h2.release()

    def test_concurrent_stress_never_lends_same_connection_twice(self):
        pool, factory = make_pool(max_size=3)
        errors = []
        stop = time.monotonic() + 0.5

        def worker(worker_id):
            try:
                while time.monotonic() < stop:
                    with pool.acquire(timeout=2, client_id=f"w{worker_id}") as handle:
                        handle.execute(f"select {worker_id}")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(5)
        self.assertEqual(errors, [])
        for conn in factory.created:
            self.assertEqual(conn.concurrent_violations, 0)
        self.assertEqual(pool.stats().checked_out, 0)


class TestPoolClose(unittest.TestCase):
    def test_close_rejects_new_acquires_and_wakes_waiters(self):
        pool, _ = make_pool(max_size=1)
        holder = pool.acquire()
        errors = []

        def waiting():
            try:
                pool.acquire(timeout=5)
            except PoolClosedError as exc:
                errors.append(exc)

        t = threading.Thread(target=waiting)
        t.start()
        time.sleep(0.1)
        pool.close()
        t.join(2)
        self.assertEqual(len(errors), 1)
        with self.assertRaises(PoolClosedError):
            pool.acquire(timeout=0)
        # 池关闭后归还的连接被直接关闭
        holder.release()
        self.assertEqual(pool.stats().total, 0)


if __name__ == "__main__":
    unittest.main()
