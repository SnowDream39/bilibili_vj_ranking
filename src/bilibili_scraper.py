import asyncio
import aiohttp
import pandas as pd
from bilibili_api import request_settings, search, video, Credential
from datetime import datetime, timedelta
import re
import random
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Literal, Any, Union
from pathlib import Path
import json
from utils.io_utils import save_to_excel
from utils.proxy import Proxy 

@dataclass
class VideoInfo:
    """视频信息数据类"""
    title: str
    bvid: str
    name: str
    author: str 
    uploader: str = ""
    copyright: int = 0
    synthesizer: str = ""
    vocal: str = ""
    type: str = ""
    pubdate: str = ""
    duration: str = ""
    page: int = 0
    view: int = 0
    favorite: int = 0
    coin: int = 0
    like: int = 0
    image_url: str = ""
    tags: str = ""
    description: str = ""
    

@dataclass
class SearchOptions:
    search_type: search.SearchObjectType = search.SearchObjectType.VIDEO
    order_type: search.OrderVideo = search.OrderVideo.PUBDATE
    video_zone_type: Optional[int] = None
    order_sort: Optional[int] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None

@dataclass
class Config:
    KEYWORDS: List[str] = field(default_factory=list)
    HEADERS: List[str] = field(default_factory=lambda: [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.67 Safari/537.36',
    ])
    MAX_RETRIES: int = 5
    SEMAPHORE_LIMIT: int = 5
    MIN_VIDEO_DURATION: int = 20
    SLEEP_TIME: int = 0.8
    OUTPUT_DIR: Path = Path("新曲数据")
    NAME: Optional[str] = None

    @staticmethod
    def load_keywords(file_path: str = "keywords.json") -> List[str]:
        with open(Path(file_path), "r", encoding="utf-8") as f:
            keywords = json.load(f)
        return keywords
    
class RetryHandler:
    """重试处理器"""
    @staticmethod
    async def retry_async(func, *args, max_retries=Config.MAX_RETRIES, **kwargs):
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(Config.SLEEP_TIME)
        return None

class BilibiliScraper:
    today: datetime = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    search_options: SearchOptions = SearchOptions()
    proxy = None
    def __init__(self, 
                 mode: Literal["new", "old", "special"], 
                 input_file: Union[str, Path] = None, 
                 days: int = 2,
                 config: Config = Config(), 
                 search_options: SearchOptions = SearchOptions(),
                 proxy: Optional[Proxy] = None,
                ):
        self.mode = mode
        self.config = config
        self.config.OUTPUT_DIR.mkdir(exist_ok=True)
        self.search_options = search_options
        self.session = None
        self.sem = asyncio.Semaphore(self.config.SEMAPHORE_LIMIT)

        if self.mode == "new":
            self.filename = self.config.OUTPUT_DIR / f"新曲{self.today.strftime('%Y%m%d')}.xlsx"
            self.start_time = self.today - timedelta(days=days)
        elif self.mode == "old":
            self.filename = self.config.OUTPUT_DIR / f"{self.today.strftime('%Y%m%d')}.xlsx"
            self.songs = pd.read_excel(input_file)
        elif self.mode == "special":
            self.filename = self.config.OUTPUT_DIR / f"{self.config.NAME}.xlsx"

        if proxy:
            request_settings.set_proxy(proxy.proxy_server)
            self.proxy = proxy
            self.config.SLEEP_TIME = 0.5
            self.config.SEMAPHORE_LIMIT = 20

    @staticmethod
    def clean_tags(text: str) -> str:
        text = re.sub(r'<.*?>', '', text)
        return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F\u200B-\u200F\u2028-\u202F\u205F-\u206F\uFEFF\uFFFE-\uFFFF]','', text)

    @staticmethod
    def convert_duration(duration: int) -> str:
        minutes, seconds = divmod(duration - 1, 60)
        return f'{minutes}分{seconds}秒' if minutes > 0 else f'{seconds}秒'

    async def get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_data(self, url: str) -> Optional[Dict]:
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': random.choice(self.config.HEADERS)}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
        return None

    async def get_video_list_by_zone(self, rid: int = 30, ps: int = 50) -> List[str]:
        """从分区获取视频，默认VU区"""
        bvids = []
        page = 1
        try:
            while True:
                if self.proxy:
                    request_settings.set_proxy(self.proxy.proxy_server) 
                url = f"https://api.bilibili.com/x/web-interface/newlist?rid={rid}&ps={ps}&pn={page}"
                 
                jsondata = await RetryHandler.retry_async(self.fetch_data, url)
                video_list = jsondata['data']['archives']
                recent_videos = [
                    video for video in video_list
                    if datetime.fromtimestamp(video['pubdate']) > self.start_time
                ]
                print(f"获取分区最新： {rid}，第 {page} 页")
                if not recent_videos:
                    break
                bvids.extend(video['bvid'] for video in recent_videos)
                page += 1
                await asyncio.sleep(self.config.SLEEP_TIME)
            return bvids

        except Exception as e:
            print('搜索分区视频时出错：', e)

    def search_by_type(self, keyword, page, search_options: SearchOptions):
        """把bilibili-api的搜索函数改一种接口"""
        return search.search_by_type(
            keyword,
            search_type= search_options.search_type,
            order_type= search_options.order_type,
            video_zone_type= search_options.video_zone_type,
            time_start= search_options.time_start,
            time_end= search_options.time_end,
            page=page
        )

    async def get_video_list_by_search(self, time_filtering: bool = False):
        """使用搜索获取新曲"""
        async def sem_fetch(keyword: str, page: int) -> Dict:
            async with self.sem:
                if self.proxy:
                    request_settings.set_proxy(self.proxy.proxy_server)  
                result = await RetryHandler.retry_async(self.search_by_type, keyword, page, self.search_options)
                videos = result.get('result', [])
                if not videos:
                    return {'end': True, 'keyword': keyword, 'bvids': []}
                bvids = []
                for item in videos:
                    if time_filtering:
                        pubdate = datetime.fromtimestamp(item['pubdate'])
                        if pubdate >= self.start_time:
                            bvids.append(item['bvid'])
                            print(f"发现视频： {item['bvid']}")
                        else:
                            return {'end': True, 'keyword': keyword, 'bvids': bvids}
                    else:
                        bvids.append(item['bvid'])
                        print(f"发现视频： {item['bvid']}")
                await asyncio.sleep(self.config.SLEEP_TIME)
                return {'end': False, 'keyword': keyword, 'bvids': bvids}
            
        keywords = self.config.KEYWORDS[:]
        page = 1
        bvids = []
        while keywords:
            print(f'正在搜索第{page}页')
            tasks = [sem_fetch(keyword, page) for keyword in keywords]
            results = await asyncio.gather(*tasks)
            for result in results:
                bvids.extend(result['bvids'])
                if result['end']:
                    keywords.remove(result['keyword'])
            await asyncio.sleep(self.config.SLEEP_TIME)

            page += 1
        return bvids
                
    async def get_all_bvids(self) -> List[str]:
        """使用搜索和分区两种方式"""
        all_bvids = set()
        
        # 从关键词搜索获取视频
        if self.mode == "new":
            bvids = await self.get_video_list_by_search(time_filtering=True)
            all_bvids.update(bvids)

            # 从分区最新获取视频
            bvids = await self.get_video_list_by_zone()
            all_bvids.update(bvids)
        else: # mode == "special"
            bvids = await self.get_video_list_by_search(time_filtering=False)
            all_bvids.update(bvids)
            
        return list(all_bvids)

    async def fetch_video_detail(self, bvid: str) -> Optional[VideoInfo]:
        """获取一个视频的详细信息"""
        if self.mode in ["new", "special"]:
            extra_info = True
        else:
            extra_info = False
        try:
            v = video.Video(bvid, credential=Credential())
            info = await v.get_info()
            if extra_info:
                tags = [tag['tag_name'] for tag in await v.get_tags()]
            
            if info['duration'] <= self.config.MIN_VIDEO_DURATION:
                print(f"跳过短视频： {bvid}")
                return None
            print(f"获取视频信息： {bvid}")
            return VideoInfo(
                title=self.clean_tags(info['title']),
                bvid=bvid,
                name=self.clean_tags(info['title']),
                author=info['owner']['name'],
                uploader=info['owner']['name'],
                copyright=info['copyright'],
                pubdate=datetime.fromtimestamp(info['pubdate']).strftime('%Y-%m-%d %H:%M:%S'),
                duration=self.convert_duration(info['duration']),
                page=len(info['pages']),
                view=info['stat']['view'],
                favorite=info['stat']['favorite'],
                coin=info['stat']['coin'],
                like=info['stat']['like'],
                image_url=info['pic'],
                tags='、'.join(tags) if extra_info else None,
                description=info['desc'] if extra_info else None,
            )
        except Exception as e:
            print(f"爬取 {bvid} 时出错: {str(e)}")
            return None

    async def update_old_songs(self, videos: List[VideoInfo]) -> None:
        """旧曲用：合并数据"""
        update_data = pd.DataFrame([{
            'bvid': video.bvid,
            'view': video.view,
            'favorite': video.favorite,
            'coin': video.coin,
            'like': video.like,
            'title': video.title,
            'uploader': video.uploader,
            'page': video.page,
            'duration': video.duration,
            'image_url': video.image_url
        } for video in videos])

        self.songs = (
        self.songs.set_index('bvid')
        .merge(update_data, on='bvid', how='left', suffixes=('', '_new'))
        .assign(**{col: lambda df, c=col: df[f"{c}_new"].combine_first(df[c]) for col in ['view', 'favorite', 'coin', 'like', 'title', 'uploader', 'page', 'duration', 'image_url']})
        .query('view > 0')
        .sort_values('view', ascending=False)
        .reset_index()
        .drop(columns=[f"{col}_new" for col in ['view', 'favorite', 'coin', 'like', 'title', 'uploader', 'page', 'duration', 'image_url']])
        )

    async def get_video_details(self, bvids: List[str]) -> List[VideoInfo]:
        """获取列表中所有视频详细信息"""
        sem = asyncio.Semaphore(self.config.SEMAPHORE_LIMIT)
        async def sem_fetch(bvid: str) -> Optional[VideoInfo]:
            async with sem:
                if self.proxy:
                    request_settings.set_proxy(self.proxy.proxy_server)  
                result = await RetryHandler.retry_async(self.fetch_video_detail, bvid)
                await asyncio.sleep(self.config.SLEEP_TIME)
                return result

        tasks = [sem_fetch(bvid) for bvid in bvids]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def process_new_songs(self) -> List[Dict[str, Any]]:
        """抓取新曲数据"""
        print("开始获取新曲数据")
        bvids = await self.get_all_bvids()
        print(f"一共有 {len(bvids)} 个 bvid")
        videos = await self.get_video_details(bvids)
        return [asdict(video) for video in videos]
    
    async def process_old_songs(self) -> List[Dict[str, Any]]:
        """抓取旧曲数据"""
        print("开始获取旧曲数据")
        bvids = self.songs['bvid'].to_list()
        videos = await self.get_video_details(bvids)
        self.update_old_songs(videos)
        return self.songs.to_dict(orient='records')

    async def save_to_excel(self, videos: List[Dict[str, Any]]) -> None:
        """导出数据"""
        df = pd.DataFrame(videos)
        df = df.sort_values(by='view', ascending=False)
        save_to_excel(df, self.filename)
