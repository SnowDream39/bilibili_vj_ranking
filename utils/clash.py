from bilibili_api import request_settings, video
import requests
import asyncio
import random
import time

from utils.proxy import Proxy

PROXY_SERVER = 'http://127.0.0.1:7897'
CONTROL_SERVER = 'http://127.0.0.1:9097'
GROUP_NAME = '肥猫云'


async def get_video(bvid) :
    v = video.Video(bvid)
    info = await v.get_info()
    print(info)

class Clash(Proxy):
    proxy_server = PROXY_SERVER
    def __init__(self):
        self.test_mode = True
        request_settings.set_proxy(PROXY_SERVER)
        self.load_proxies()

    def switch_proxy(self, proxy):
        url = CONTROL_SERVER + '/proxies/' + GROUP_NAME
        data = {'name': proxy}
        requests.put(url, json=data)

    def proxy_valid(self, proxy):
        if any(keyword in proxy for keyword in ['自动选择','故障转移','最新网址', '剩余流量', '距离下次重置', '套餐到期']):
            return False

        try:
            self.switch_proxy(proxy)
            response = requests.get('https://api.bilibili.com/x/web-interface/wbi/view',
                          {'bvid': 'BV1MtPceEE5R'},
                          timeout=0.3)
            print(f'节点{proxy}有效')
            return True
        except Exception as e:
            print(f'节点{proxy}出错：', e)
            return False


    def load_proxies(self):
        try:
            response = requests.get(CONTROL_SERVER + '/proxies/肥猫云')
            all_proxies = response.json()['all']
            self.proxies = [proxy for proxy in all_proxies if self.proxy_valid(proxy)]
            print(self.proxies)
        except Exception as e:
            print(e)

    def random_proxy(self):
        proxy = random.choice(self.proxies)
        self.switch_proxy(proxy)





