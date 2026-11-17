"""线程安全的数据库连接池核心。

设计要点：
- 连接创建与查询执行通过注入的 ConnectionFactory / Connection 接口完成。
- 事务期间连接被固定（pin）给同一个调用方，commit/rollback 后归还。
- 等待队列按 FIFO + 每个请求的 deadline 分配，过期的等待者不会抢到连接；
  可选的 max_per_client 限制避免同一客户端长期占满池。
- generation 单调递增；failover 时 bump_generation()，旧 epoch 的连接
  不允许执行新查询，归还时直接关闭而不是放回池中。
- 健康检查对瞬时失败做有限次重试，避免把仍可恢复的连接误判关掉。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterator, List, Optional

from .errors import (
    AcquireTimeoutError,
    ClientLimitError,
    ConnectionReleasedError,
    PoolClosedError,
    StaleConnectionError,
)
from .interfaces import Connection, ConnectionFactory


class _Entry:
    """池内部对一条连接的生命周期记录。"""

    __slots__ = ("conn", "generation", "checked_out_at", "holder")

    def __init__(self, conn: Connection, generation: int) -> None:
        self.conn = conn
        self.generation = generation
        self.checked_out_at: Optional[float] = None
        self.holder: Optional[str] = None


class _Waiter:
    """等待队列中的一个请求，携带自己的 deadline。"""

    __slots__ = ("deadline",)

    def __init__(self, deadline: Optional[float]) -> None:
        self.deadline = deadline

    def remaining(self) -> Optional[float]:
        if self.deadline is None:
            return None
        return self.deadline - time.monotonic()


@dataclass(frozen=True)
class PoolStats:
    total: int
    idle: int
    checked_out: int
    waiters: int
    generation: int


@dataclass(frozen=True)
class LeakInfo:
    holder: Optional[str]
    checked_out_for: float


@dataclass(frozen=True)
class HealthReport:
    healthy: int
    dropped: int


class PooledConnection:
    """借出连接的句柄。release 之后不可再使用。"""

    def __init__(self, pool: "ConnectionPool", entry: _Entry, client_id: Optional[str]) -> None:
        self._pool = pool
        self._entry = entry
        self._client_id = client_id
        self._released = False

    @property
    def raw(self) -> Connection:
        """底层连接（仅供测试/适配层使用）。"""
        return self._entry.conn

    @property
    def generation(self) -> int:
        return self._entry.generation

    def _ensure_usable(self) -> None:
        if self._released:
            raise ConnectionReleasedError("connection handle already released")
        if self._entry.generation != self._pool.generation:
            raise StaleConnectionError(
                f"connection belongs to generation {self._entry.generation}, "
                f"current generation is {self._pool.generation}"
            )

    def execute(self, query: str) -> Any:
        self._ensure_usable()
        return self._entry.conn.execute(query)

    def ping(self) -> None:
        self._ensure_usable()
        self._entry.conn.ping()

    def commit(self) -> None:
        self._ensure_usable()
        self._entry.conn.commit()

    def rollback(self) -> None:
        self._ensure_usable()
        self._entry.conn.rollback()

    def release(self, broken: bool = False) -> None:
        """归还连接；幂等。broken=True 表示连接状态不可信，直接关闭。"""
        if self._released:
            return
        self._released = True
        self._pool._release(self._entry, self._client_id, broken=broken)

    def __enter__(self) -> "PooledConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release(broken=exc_type is not None)


class ConnectionPool:
    def __init__(
        self,
        factory: ConnectionFactory,
        max_size: int = 10,
        max_per_client: Optional[int] = None,
        health_check_attempts: int = 3,
        health_check_retry_delay: float = 0.05,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if max_per_client is not None and max_per_client < 1:
            raise ValueError("max_per_client must be >= 1")
        if health_check_attempts < 1:
            raise ValueError("health_check_attempts must be >= 1")
        self._factory = factory
        self._max_size = max_size
        self._max_per_client = max_per_client
        self._health_check_attempts = health_check_attempts
        self._health_check_retry_delay = health_check_retry_delay

        self._cond = threading.Condition()
        self._idle: List[_Entry] = []
        self._checked_out: Dict[int, _Entry] = {}
        self._waiters: Deque[_Waiter] = deque()
        self._client_holds: Dict[str, int] = {}
        self._total = 0
        self._generation = 0
        self._closed = False

    # ------------------------------------------------------------------
    # 基本属性
    # ------------------------------------------------------------------

    @property
    def generation(self) -> int:
        with self._cond:
            return self._generation

    @property
    def max_size(self) -> int:
        return self._max_size

    def stats(self) -> PoolStats:
        with self._cond:
            return PoolStats(
                total=self._total,
                idle=len(self._idle),
                checked_out=len(self._checked_out),
                waiters=len(self._waiters),
                generation=self._generation,
            )

    # ------------------------------------------------------------------
    # 借出 / 归还
    # ------------------------------------------------------------------

    def acquire(self, timeout: Optional[float] = None, client_id: Optional[str] = None) -> PooledConnection:
        """借出一条连接。

        timeout 是本次请求的时限（秒）；超时抛出 AcquireTimeoutError。
        等待按 FIFO 排队，只有队首的等待者能拿到空闲连接或新建名额，
        过期请求会被移出队列，不会事后抢到连接。
        """
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be >= 0")
        deadline = None if timeout is None else time.monotonic() + timeout
        waiter = _Waiter(deadline)
        enqueued = False

        with self._cond:
            self._ensure_open()
            self._check_client_limit(client_id)
            while True:
                if self._closed:
                    if enqueued:
                        self._discard_waiter(waiter)
                    raise PoolClosedError("pool is closed")
                if not enqueued:
                    if not self._waiters:
                        entry = self._pop_idle_locked()
                        if entry is not None:
                            self._adopt_locked(entry, client_id)
                            return PooledConnection(self, entry, client_id)
                        if self._total < self._max_size:
                            self._total += 1  # 预留名额，锁外建连
                            break
                    self._waiters.append(waiter)
                    enqueued = True
                elif self._waiters and self._waiters[0] is waiter:
                    entry = self._pop_idle_locked()
                    if entry is not None:
                        self._waiters.popleft()
                        self._adopt_locked(entry, client_id)
                        return PooledConnection(self, entry, client_id)
                    if self._total < self._max_size:
                        self._waiters.popleft()
                        self._total += 1
                        break
                remaining = waiter.remaining()
                if remaining is not None and remaining <= 0:
                    self._discard_waiter(waiter)
                    raise AcquireTimeoutError(
                        f"no connection available within {timeout} seconds"
                    )
                self._cond.wait(remaining)

        # 已预留名额，在锁外创建连接，避免慢 connect 阻塞整个池。
        try:
            conn = self._factory.connect()
        except Exception:
            with self._cond:
                self._total -= 1
                self._cond.notify_all()
            raise
        with self._cond:
            if self._closed:
                self._total -= 1
                self._cond.notify_all()
                conn.close()
                raise PoolClosedError("pool is closed")
            entry = _Entry(conn, self._generation)
            self._adopt_locked(entry, client_id)
            return PooledConnection(self, entry, client_id)

    def _release(self, entry: _Entry, client_id: Optional[str], broken: bool = False) -> None:
        with self._cond:
            if id(entry) not in self._checked_out:
                return  # 重复归还直接忽略，保证一条连接不会回到池里两次
            del self._checked_out[id(entry)]
            entry.checked_out_at = None
            entry.holder = None
            if client_id is not None:
                holds = self._client_holds.get(client_id, 0) - 1
                if holds > 0:
                    self._client_holds[client_id] = holds
                else:
                    self._client_holds.pop(client_id, None)
            stale = entry.generation != self._generation
            if broken or stale or self._closed:
                # 旧 epoch / 损坏 / 池关闭：不回收，直接关闭并腾出名额
                self._total -= 1
                entry.conn.close()
            else:
                self._idle.append(entry)
            self._cond.notify_all()

    # ------------------------------------------------------------------
    # 事务
    # ------------------------------------------------------------------

    @contextmanager
    def transaction(self, timeout: Optional[float] = None, client_id: Optional[str] = None) -> Iterator[PooledConnection]:
        """事务上下文：整个事务固定同一条连接，提交或回滚后归还。"""
        handle = self.acquire(timeout=timeout, client_id=client_id)
        try:
            yield handle
        except Exception:
            try:
                handle.rollback()
            finally:
                handle.release()
            raise
        try:
            handle.commit()
        except Exception:
            # 提交失败，连接状态不可信，关闭而不是回收
            handle.release(broken=True)
            raise
        handle.release()

    # ------------------------------------------------------------------
    # Failover / generation
    # ------------------------------------------------------------------

    def bump_generation(self) -> int:
        """进入新 epoch：空闲的旧连接立即关闭；借出的旧连接不能执行新查询，
        归还时关闭。新借出的连接由注入的 factory 创建（此时 factory 应指向新主库）。
        """
        with self._cond:
            self._generation += 1
            stale = [e for e in self._idle if e.generation != self._generation]
            for e in stale:
                self._idle.remove(e)
                self._total -= 1
            self._cond.notify_all()
            generation = self._generation
        for e in stale:
            e.conn.close()
        return generation

    def failover(self) -> int:
        """主库切换的语义化入口，等价于 bump_generation()。"""
        return self.bump_generation()

    # ------------------------------------------------------------------
    # 健康检查
    # ------------------------------------------------------------------

    def health_check(self) -> HealthReport:
        """探活空闲连接。瞬时失败会重试 health_check_attempts 次，
        全部失败才丢弃，避免把仍可恢复的连接误判关掉。
        """
        healthy = 0
        dropped = 0
        with self._cond:
            candidates = list(self._idle)
        for entry in candidates:
            with self._cond:
                if entry not in self._idle:
                    continue  # 已被借出，跳过
            if self._ping_with_retries(entry.conn):
                healthy += 1
                continue
            with self._cond:
                if entry in self._idle:
                    self._idle.remove(entry)
                    self._total -= 1
                    dropped += 1
                    self._cond.notify_all()
            entry.conn.close()
        return HealthReport(healthy=healthy, dropped=dropped)

    def _ping_with_retries(self, conn: Connection) -> bool:
        for attempt in range(self._health_check_attempts):
            try:
                conn.ping()
                return True
            except Exception:
                if attempt + 1 < self._health_check_attempts:
                    time.sleep(self._health_check_retry_delay)
        return False

    # ------------------------------------------------------------------
    # 泄漏检测与关闭
    # ------------------------------------------------------------------

    def detect_leaks(self, max_age: float) -> List[LeakInfo]:
        """返回借出时间超过 max_age 秒的连接信息。"""
        now = time.monotonic()
        with self._cond:
            return [
                LeakInfo(holder=e.holder, checked_out_for=now - (e.checked_out_at or now))
                for e in self._checked_out.values()
                if e.checked_out_at is not None and now - e.checked_out_at > max_age
            ]

    def close(self) -> None:
        """关闭池：空闲连接立即关闭；借出的连接归还时关闭；等待者收到 PoolClosedError。"""
        with self._cond:
            if self._closed:
                return
            self._closed = True
            idle, self._idle = self._idle, []
            self._total -= len(idle)
            self._cond.notify_all()
        for e in idle:
            e.conn.close()

    # ------------------------------------------------------------------
    # 内部工具（需在锁内调用，除特别说明外）
    # ------------------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed:
            raise PoolClosedError("pool is closed")

    def _check_client_limit(self, client_id: Optional[str]) -> None:
        if client_id is None or self._max_per_client is None:
            return
        if self._client_holds.get(client_id, 0) >= self._max_per_client:
            raise ClientLimitError(
                f"client {client_id!r} already holds {self._max_per_client} connection(s)"
            )

    def _pop_idle_locked(self) -> Optional[_Entry]:
        while self._idle:
            entry = self._idle.pop()
            if entry.generation != self._generation:
                # 旧 epoch 的空闲连接不回收，直接关闭
                self._total -= 1
                entry.conn.close()
                continue
            return entry
        return None

    def _adopt_locked(self, entry: _Entry, client_id: Optional[str]) -> None:
        entry.checked_out_at = time.monotonic()
        entry.holder = client_id
        self._checked_out[id(entry)] = entry
        if client_id is not None:
            self._client_holds[client_id] = self._client_holds.get(client_id, 0) + 1

    def _discard_waiter(self, waiter: _Waiter) -> None:
        try:
            self._waiters.remove(waiter)
        except ValueError:
            pass
