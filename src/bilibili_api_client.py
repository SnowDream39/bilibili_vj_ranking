# src/bilibili_api_client.py
import asyncio
import aiohttp
from bilibili_api import request_settings, search
from datetime import datetime
import random
from typing import List, Optional, Dict, Any, Set

from utils.logger import logger
from utils.proxy import Proxy 
from utils.retry_handler import RetryHandler
from utils.dataclass import Config, SearchOptions, SearchRestrictions

class BilibiliApiClient:
    """
    B站API客户端，负责所有网络请求。
    """
    def __init__(self, config: Config, proxy: Optional[Proxy] = None):
        self.config = config
        self.proxy = proxy
        self.session: Optional[aiohttp.ClientSession] = None
        self.sem = asyncio.Semaphore(config.SEMAPHORE_LIMIT)
        self.retry_handler = RetryHandler(config.MAX_RETRIES, config.SLEEP_TIME)

        if self.proxy:
            request_settings.set_proxy(self.proxy.proxy_server)

    async def get_session(self):
        """获取或创建 aiohttp 会话以实现复用。"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close_session(self):
        """关闭 aiohttp 会话以释放资源。"""
        if self.session:
            await self.session.close()
            self.session = None

    async def _fetch_json(self, url: str) -> Optional[Dict[str, Any]]:
        """通用的异步HTTP GET请求函数。"""
        session = await self.get_session()
        headers = {'User-Agent': random.choice(self.config.HEADERS)}
        proxy_url = self.proxy.proxy_server if self.proxy else None
        async with session.get(url, headers=headers, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"HTTP 请求失败，状态码: {response.status}")

    def _search_by_type(self, keyword: str, page: int, options: SearchOptions):
        """封装 bilibili-api 的搜索功能。"""
        return search.search_by_type(
            keyword,
            search_type=options.search_type,
            order_type=options.order_type,
            video_zone_type=options.video_zone_type,
            time_start=options.time_start,
            time_end=options.time_end,
            page=page,
            page_size=options.page_size or 50
        )

    async def get_aids_from_newlist(self, rid: int, ps: int, start_time: datetime) -> List[str]:
        """通过分区最新API获取aid列表。"""
        aids: Set[str] = set()
        page = 1
        try:
            while True:
                url = f"https://api.bilibili.com/x/web-interface/newlist?rid={rid}&ps={ps}&pn={page}"
                jsondata = await self.retry_handler.retry_async(self._fetch_json, url)
                if jsondata and jsondata.get('data'):
                    video_list = jsondata['data']['archives']
                    recent_videos = [v for v in video_list if datetime.fromtimestamp(v['pubdate']) > start_time]
                    logger.info(f"获取分区最新： {rid}，第 {page} 页，新增{len(recent_videos)} 个")
                    if not recent_videos: break
                    aids.update(str(v['aid']) for v in recent_videos)
                    page += 1
                    await asyncio.sleep(self.config.SLEEP_TIME)
                else:
                    break
            return list(set(aids))
        except Exception as e:
            logger.error(f'搜索分区视频时出错：{e}')
            return list(set(aids))

    async def get_aids_from_search(self, keywords: List[str], search_options: SearchOptions, restrictions: Optional[SearchRestrictions]) -> List[str]:
        """通过关键词搜索获取aid列表。"""
        aids: Set[str] = set()
        batch_size = 3
        keyword_pages = {kw: 1 for kw in keywords}
        active_keywords = keywords[:]

        while active_keywords:
            current_batch = active_keywords[:batch_size]
            logger.info(f'[分区 {search_options.video_zone_type}] 处理关键词批次: {current_batch}')

            async def sem_fetch(keyword: str) -> Dict[str, Any]:
                async with self.sem:
                    result = await self.retry_handler.retry_async(self._search_by_type, keyword, keyword_pages[keyword], search_options)
                    if not result or 'result' not in result:
                        return {'end': True, 'keyword': keyword, 'aids': []}
                    
                    videos = result.get('result', [])
                    end = not videos or len(videos) < (search_options.page_size or 50)
                    temp_aids = []
                    for item in videos:
                        if restrictions:
                            if restrictions.min_favorite and item['favorites'] < restrictions.min_favorite: return {'end': True, 'keyword': keyword, 'aids': temp_aids}
                            if restrictions.min_view and item['play'] < restrictions.min_view: return {'end': True, 'keyword': keyword, 'aids': temp_aids}
                        temp_aids.append(str(item['aid']))
                    return {'end': end, 'keyword': keyword, 'aids': temp_aids}

            tasks = [sem_fetch(keyword) for keyword in current_batch]
            results = await asyncio.gather(*tasks)

            for result in results:
                keyword = result['keyword']
                found_count = len(result['aids'])
                aids.update(result['aids'])
                logger.info(f"    关键词 '{keyword}': 新增 {found_count} 个")

                if result['end']:
                    if keyword in active_keywords: active_keywords.remove(keyword)
                else:
                    keyword_pages[keyword] += 1
            
            if current_batch:
                remaining = [k for k in active_keywords if k not in current_batch]
                active_keywords = remaining + [k for k in current_batch if k in active_keywords]
            await asyncio.sleep(1)
        return list(set(aids))

    async def get_batch_details_by_aid(self, aids: List[int]) -> Dict[int, Dict[str, Any]]:
        """使用medialist接口批量获取视频详细信息。"""
        BATCH_SIZE = 50
        all_stats = {}
        for i in range(0, len(aids), BATCH_SIZE):
            batch_aids = aids[i:i + BATCH_SIZE]
            resources_str = ",".join([f"{aid}:2" for aid in batch_aids])
            url = f"https://api.bilibili.com/medialist/gateway/base/resource/infos?resources={resources_str}"
            logger.info(f"正在通过 medialist 接口处理批次 {i//BATCH_SIZE + 1}...")
            try:
                jsondata = await self._fetch_json(url)
                if jsondata and jsondata.get('code') == 0 and jsondata.get('data'):
                    for item in jsondata['data']:
                        all_stats[item['id']] = item
                else:
                    logger.warning(f"API 返回错误或无数据，批次：{batch_aids}")
                await asyncio.sleep(self.config.SLEEP_TIME)
            except Exception as e:
                logger.error(f"处理批次时发生异常: {e}, 批次: {batch_aids}")
        return all_stats

    async def get_videos_from_newlist_rank(self, cate_id: int, time_from: str, time_to: str) -> List[Dict[str, Any]]:
        """
        分页获取热门榜视频，内置局部重试。
        """
        all_videos: List[Dict[str, Any]] = []
        page = 1
        
        while True:
            url = (
                f"https://api.bilibili.com/x/web-interface/newlist_rank?"
                f"main_ver=v3&search_type=video&view_type=hot_rank&copy_right=-1&order=click"
                f"&cate_id={cate_id}&page={page}&pagesize=50&time_from={time_from}&time_to={time_to}"
            )
            
            logger.info(f"{time_from}~{time_to}, 第 {page} 页...")
            
            jsondata = None
            for attempt in range(self.config.MAX_RETRIES):
                response = await self._fetch_json(url)
                if response and response.get('code') == 0 and (response.get('data', {}).get('result') or page > 1):
                    jsondata = response
                    break
                await asyncio.sleep(1.5 ** attempt)
            else: 
                logger.error(f"第{page}页 重试失败，放弃该时段")
                break 
            videos = jsondata.get('data', {}).get('result')
            if not videos:
                break

            all_videos.extend(videos)
            page += 1
            await asyncio.sleep(self.config.SLEEP_TIME)
        return all_videos