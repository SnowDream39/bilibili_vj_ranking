# utils/retry_handler.py
# 提供通用的重试逻辑，支持同步和异步函数。
import asyncio
import time
from utils.logger import logger
from typing import TypeVar, Callable, Awaitable

# 定义一个泛型类型 T
T = TypeVar('T')

class RetryHandler:
    """提供同步和异步函数的自动重试机制。"""
    max_retries: int
    sleep_time: float
    
    def __init__(self, max_retries: int = 10, sleep_time: float = 0.5) -> None:
        """初始化重试处理器。

        Args:
            max_retries (int): 默认的最大重试次数。
            sleep_time (float): 每次重试之间的默认等待时间（秒）。
        """
        self.max_retries = max_retries
        self.sleep_time = sleep_time

    def retry(self, func: Callable[..., T], *args, max_retries = None, **kwargs) -> T:
        """为同步函数提供重试逻辑。

        Args:
            func (Callable[..., T]): 需要重试的同步函数。
            *args: 传递给函数的位置参数。
            max_retries (int, optional): 本次调用的最大重试次数，会覆盖实例的默认值。
            **kwargs: 传递给函数的关键字参数。

        Returns:
            T: 成功时返回原函数的返回值。

        Raises:
            Exception: 在所有尝试都失败后，抛出此异常。
        """
        if not max_retries:
            max_retries = self.max_retries
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"第 {attempt + 1}/{max_retries} 次尝试失败: {str(e)}")  
                time.sleep(self.sleep_time)

        logger.error(f"超过最大重试次数，放弃请求")
        raise Exception("超过最大重试次数")

    async def retry_async(self, func: Callable[..., Awaitable[T]], *args, max_retries = None, **kwargs) -> T:
        """为异步函数提供重试逻辑。

        Args:
            func (Callable[..., Awaitable[T]]): 需要重试的异步函数。
            *args: 传递给函数的位置参数。
            max_retries (int, optional): 本次调用的最大重试次数，会覆盖实例的默认值。
            **kwargs: 传递给函数的关键字参数。

        Returns:
            T: 成功时返回原函数的返回值。

        Raises:
            Exception: 在所有尝试都失败后，抛出此异常。
        """
        if not max_retries:
            max_retries = self.max_retries
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"第 {attempt + 1}/{max_retries} 次尝试失败: {str(e)}")  
                await asyncio.sleep(self.sleep_time)
                
        logger.error(f"超过最大重试次数，放弃请求")
        raise Exception("超过最大重试次数")