"""
这是代理的基类，任何代理都必须继承这个类，并实现里面的方法
"""


from abc import ABC, abstractmethod

class Proxy(ABC):
    proxy_server: str

    @abstractmethod
    def random_proxy(self):
        pass