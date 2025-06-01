import asyncio
import time
from utils.logger import logger
from typing import TypeVar, Callable, Awaitable


# 定义一个泛型类型 T
T = TypeVar('T')

class RetryHandler:
    max_retries: int
    sleep_time: float
    
    def __init__(self, max_retries: int = 10, sleep_time: float = 0.5) -> None:
        self.max_retries = max_retries
        self.sleep_time = sleep_time

    """重试处理器"""
    def retry(self, func: Callable[..., T], *args, max_retries = None, **kwargs) -> T:
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

    """异步重试处理器"""
    async def retry_async(self, func: Callable[..., Awaitable[T]], *args, max_retries = None, **kwargs) -> T:
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