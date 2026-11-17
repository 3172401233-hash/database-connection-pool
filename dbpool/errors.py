"""连接池异常类型。"""


class PoolError(Exception):
    """连接池相关异常的基类。"""


class AcquireTimeoutError(PoolError):
    """在请求时限内没有等到可用连接。"""


class PoolClosedError(PoolError):
    """连接池已关闭。"""


class StaleConnectionError(PoolError):
    """连接属于旧 generation（旧 epoch），不允许执行新查询。"""


class ConnectionReleasedError(PoolError):
    """连接句柄已归还，不能继续使用。"""


class ClientLimitError(PoolError):
    """同一客户端持有的连接数超过上限。"""
