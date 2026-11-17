"""注入接口：连接的创建与查询执行都通过这些协议注入，池本身不依赖任何具体驱动。"""

from typing import Any, Protocol


class Connection(Protocol):
    """池所依赖的最小连接接口，由具体驱动/适配器实现。"""

    def execute(self, query: str) -> Any:
        """执行查询并返回结果。"""
        ...

    def ping(self) -> None:
        """健康检查探活；失败时抛出异常。"""
        ...

    def commit(self) -> None:
        """提交当前事务。"""
        ...

    def rollback(self) -> None:
        """回滚当前事务。"""
        ...

    def close(self) -> None:
        """关闭底层连接。必须幂等。"""
        ...


class ConnectionFactory(Protocol):
    """连接工厂接口，failover 时由注入方切换到新主库。"""

    def connect(self) -> Connection:
        """创建一个新连接。"""
        ...
