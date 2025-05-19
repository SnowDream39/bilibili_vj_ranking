import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MyProxy:
    def __init__(self, proxy_dir: str = "proxies"):
        self.proxy_dir = Path(proxy_dir)
        if not self.proxy_dir.exists() or not self.proxy_dir.is_dir():
            raise FileNotFoundError(f"代理目录 {self.proxy_dir} 不存在")
        
        self.proxies = []  
        self.current_index = 0  
        self.load_proxies()

    def load_proxy_config(self, proxy_file: Path):
        if not proxy_file.exists():
            raise FileNotFoundError(f"代理配置文件 {proxy_file} 不存在")
        
        with open(proxy_file, "r", encoding="utf-8") as file:
            file.seek(0)  
            return json.load(file)

    def load_proxies(self):
        proxy_files = list(self.proxy_dir.glob("*.json"))
        if not proxy_files:
            raise ValueError(f"未找到任何代理配置文件在目录 {self.proxy_dir}")
        
        for proxy_file in proxy_files:
            try:
                proxy_config = self.load_proxy_config(proxy_file)
                socks_inbound = next(
                    (inbound for inbound in proxy_config.get("inbounds", []) if inbound["protocol"] == "socks"),
                    None
                )
                if socks_inbound:
                    socks_host = socks_inbound["listen"]
                    socks_port = socks_inbound["port"]
                    proxy_url = f"socks5://{socks_host}:{socks_port}"
                    logging.info(f"发现代理节点: {proxy_url}")
                    self.proxies.append(proxy_url)
                else:
                    logging.warning(f"代理文件 {proxy_file} 中未找到 SOCKS5 配置")
            except Exception as e:
                logging.error(f"加载代理文件 {proxy_file} 失败: {e}")
        
        if not self.proxies:
            raise ValueError("未找到任何有效的 SOCKS5 代理节点")

    def next_proxy(self) -> str:
        if not self.proxies:
            raise ValueError("没有可用的代理节点")
        proxy = self.proxies[self.current_index]
        logging.info(f"已切换到代理节点: {proxy}")
        self.current_index = (self.current_index + 1) % len(self.proxies) 
        return proxy

    @property
    def proxy_server(self) -> str:
        return self.next_proxy()  
