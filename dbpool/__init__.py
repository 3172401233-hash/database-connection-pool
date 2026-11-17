"""dbpool：注入式接口的数据库连接池。"""

from .errors import (
    AcquireTimeoutError,
    ClientLimitError,
    ConnectionReleasedError,
    PoolClosedError,
    PoolError,
    StaleConnectionError,
)
from .interfaces import Connection, ConnectionFactory
from .pool import ConnectionPool, HealthReport, LeakInfo, PooledConnection, PoolStats

__all__ = [
    "AcquireTimeoutError",
    "ClientLimitError",
    "Connection",
    "ConnectionFactory",
    "ConnectionPool",
    "ConnectionReleasedError",
    "HealthReport",
    "LeakInfo",
    "PoolClosedError",
    "PoolError",
    "PooledConnection",
    "PoolStats",
    "StaleConnectionError",
]
