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
from utils.logger import logger
from utils.io_utils import save_to_excel
from utils.proxy import Proxy 
from utils.retry_handler import RetryHandler

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
    tags: Optional[str] = None
    description: Optional[str] = None
    streak : int = 0
    

@dataclass
class SearchOptions:
    """搜索时使用的B站搜索参数配置"""
    search_type: search.SearchObjectType = search.SearchObjectType.VIDEO
    order_type: search.OrderVideo = search.OrderVideo.PUBDATE
    video_zone_type: Optional[List[int]] = None
    order_sort: Optional[int] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None

@dataclass
class Config:
    """爬虫全局配置"""
    KEYWORDS: List[str] = field(default_factory=list)
    HEADERS: List[str] = field(default_factory=lambda: [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.67 Safari/537.36',
    ])
    MAX_RETRIES: int = 5
    SEMAPHORE_LIMIT: int = 5
    MIN_VIDEO_DURATION: int = 20
    SLEEP_TIME: float = 0.8
    OUTPUT_DIR: Path = Path("新曲数据")
    NAME: Optional[str] = None
    STREAK_THRESHOLD : int = 7
    MIN_TOTAL_VIEW: int = 10000
    BASE_THRESHOLD: int = 100 

    @staticmethod
    def load_keywords(file_path: str = "keywords.json") -> List[str]:
        with open(Path(file_path), "r", encoding="utf-8") as f:
            keywords = json.load(f)
        return keywords
    
class BilibiliScraper:
    """B站视频爬虫"""
    # 根据当前时间确定文件名，23点后次日处理
    today: datetime = (datetime.now() + timedelta(days=1) if datetime.now().hour >= 23 else datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    search_options: SearchOptions = SearchOptions()
    proxy = None
    def __init__(self, 
                 mode: Literal["new", "old", "special"], 
                 input_file: Union[str, Path, None] = None, 
                 days: int = 2,
                 config: Config = Config(), 
                 search_options: SearchOptions = SearchOptions(),
                 proxy: Optional[Proxy] = None,
                ):
        """
        Args:
            input_file: 输入文件路径
        """
        self.mode = mode
        self.config = config
        self.config.OUTPUT_DIR.mkdir(exist_ok=True)
        self.search_options = search_options
        self.session = None
        self.sem = asyncio.Semaphore(self.config.SEMAPHORE_LIMIT)
        self.retry_handler = RetryHandler(Config.MAX_RETRIES, Config.SLEEP_TIME)

        if self.mode == "new":
            self.filename = self.config.OUTPUT_DIR / f"新曲{self.today.strftime('%Y%m%d')}.xlsx"
            self.start_time = self.today - timedelta(days=days)
        elif self.mode == "old":
            self.filename = self.config.OUTPUT_DIR / f"{self.today.strftime('%Y%m%d')}.xlsx"
            self.songs = pd.read_excel(input_file)
            if 'streak' not in self.songs.columns:
                self.songs['streak'] = 0
        elif self.mode == "special":
            self.filename = self.config.OUTPUT_DIR / f"{self.config.NAME}.xlsx"

        if proxy:
            request_settings.set_proxy(proxy.proxy_server)
            self.proxy = proxy
            self.config.SLEEP_TIME = 0.5
            self.config.SEMAPHORE_LIMIT = 20
            
    # ==================== 辅助函数 ====================
    def is_census_day(self) -> bool:
            """判断是否为普查日（周六或每月1日）"""
            return (self.today.weekday() == 5) or (self.today.day == 1)
    
    def calculate_dynamic_threshold(self, streak: int) -> int:
        """计算动态阈值"""
        if streak <= self.config.STREAK_THRESHOLD:
            return self.config.BASE_THRESHOLD
        gap_days = streak - self.config.STREAK_THRESHOLD
        return self.config.BASE_THRESHOLD * (gap_days + 1)
    
    @staticmethod
    def clean_tags(text: str) -> str:
        """清理HTML标签和特殊字符"""
        text = re.sub(r'<.*?>', '', text)
        return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F\u200B-\u200F\u2028-\u202F\u205F-\u206F\uFEFF\uFFFE-\uFFFF]','', text)

    @staticmethod
    def convert_duration(duration: int) -> str:
        """规范化视频时长"""
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

    def search_by_type(self, keyword, page, search_options: SearchOptions):
        """调用bilibili-api的搜索，并改变接口"""
        return search.search_by_type(
            keyword,
            search_type= search_options.search_type,
            order_type= search_options.order_type,
            video_zone_type= search_options.video_zone_type,
            time_start= search_options.time_start,
            time_end= search_options.time_end,
            page=page
        )
    
    async def fetch_data(self, url: str) -> Optional[Dict]:
        """请求API获取JSON数据"""
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': random.choice(self.config.HEADERS)}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
        return None
    


    # =================== 获取视频列表 ===================
    # 第一种方式，在分区内获取
    async def get_video_list_by_zone(self, rid: int = 30, ps: int = 50) -> List[str]:
        """获取分区最新视频列表，默认分区为30"""
        bvids: List[str] = []
        page = 1
        try:
            while True:
                if self.proxy:
                    request_settings.set_proxy(self.proxy.proxy_server) 
                url = f"https://api.bilibili.com/x/web-interface/newlist?rid={rid}&ps={ps}&pn={page}"
                
                jsondata = await self.retry_handler.retry_async(self.fetch_data, url)
                if (jsondata):
                    video_list = jsondata['data']['archives']
                    recent_videos = [
                        video for video in video_list
                        if datetime.fromtimestamp(video['pubdate']) > self.start_time
                    ]
                    logger.info(f"获取分区最新： {rid}，第 {page} 页")
                    if not recent_videos:
                        break
                    bvids.extend(video['bvid'] for video in recent_videos)
                    page += 1
                    await asyncio.sleep(self.config.SLEEP_TIME)
                else:
                    raise Exception("获取数据失败")
            return bvids

        except Exception as e:
            logger.error('搜索分区视频时出错：', e)
            return bvids

    # 第二种方式，在搜索中获取
    async def get_video_list_by_search(self, time_filtering: bool = False) -> List[str]:
        """根据关键词获取视频列表"""
        all_bvids = set()
        bvids = await self.get_video_list_by_search_for_zone(self.search_options.video_zone_type, time_filtering=time_filtering)
        all_bvids.update(bvids)

        return list(all_bvids)
    
    async def get_video_list_by_search_for_zone(self, zone: Optional[List[int]], time_filtering: bool = False) -> List[str]:
        """搜索分区下指定关键词的视频"""
        keywords = self.config.KEYWORDS[:]
        bvids = []
        batch_size = 5
        keyword_pages = {keyword: 1 for keyword in keywords} 
        active_keywords = keywords[:] 

        while active_keywords:
            current_batch = active_keywords[:batch_size]
            logger.info(f'[分区 {zone}] 处理关键词批次: {current_batch}')

            async def sem_fetch(keyword: str) -> Dict:
                async with self.sem:
                    if self.proxy:
                        request_settings.set_proxy(self.proxy.proxy_server)
                    search_opts = SearchOptions(
                        search_type=self.search_options.search_type,
                        order_type=self.search_options.order_type,
                        video_zone_type=zone,
                        time_start=self.search_options.time_start,
                        time_end=self.search_options.time_end
                    )
                    result = await self.retry_handler.retry_async(
                        self.search_by_type, 
                        keyword, 
                        keyword_pages[keyword], 
                        search_opts
                    )
                    
                    if result:
                        videos = result.get('result', [])
                        if not videos:
                            return {'end': True, 'keyword': keyword, 'bvids': []}
                    else:
                        raise Exception("搜索结果为空失败")
                    
                    temp_bvids = []
                    for item in videos:
                        if time_filtering:
                            pubdate = datetime.fromtimestamp(item['pubdate'])
                            if pubdate >= self.start_time:
                                temp_bvids.append(item['bvid'])
                                logger.info(f"[分区 {zone}] 发现视频: {item['bvid']} (关键词 {keyword} 第{keyword_pages[keyword]}页)")
                            else:
                                return {'end': True, 'keyword': keyword, 'bvids': temp_bvids}
                        else:
                            temp_bvids.append(item['bvid'])
                            logger.info(f"[分区 {zone}] 发现视频: {item['bvid']} (关键词 {keyword} 第{keyword_pages[keyword]}页)")
                    
                    await asyncio.sleep(self.config.SLEEP_TIME)
                    return {'end': False, 'keyword': keyword, 'bvids': temp_bvids}

            tasks = [sem_fetch(keyword) for keyword in current_batch]
            results = await asyncio.gather(*tasks)

            for result in results:
                keyword = result['keyword']
                bvids.extend(result['bvids'])
                if result['end']:
                    if keyword in active_keywords:
                        active_keywords.remove(keyword)
                else:
                    keyword_pages[keyword] += 1
            if current_batch:
                remaining_keywords = [k for k in active_keywords if k not in current_batch]
                active_keywords = remaining_keywords + [k for k in current_batch if k in active_keywords]

            await asyncio.sleep(self.config.SLEEP_TIME)

        return list(set(bvids))

    async def fetch_video_detail(self, bvid: str) -> Optional[VideoInfo]:
        """获取单个视频的详细信息"""
        try:
            existing_data = {}
            if self.mode == "old":
                song_data = self.songs[self.songs['bvid'] == bvid].iloc[0]
                existing_data = {
                    'name': song_data['name'],
                    'bvid': bvid,
                    'author': song_data['author'],
                    'synthesizer': song_data['synthesizer'],
                    'vocal': song_data['vocal'],
                    'copyright': song_data['copyright'],
                    'type': song_data['type'],
                    'pubdate': song_data['pubdate'],
                }
            v = video.Video(bvid, credential=Credential())
            info = await v.get_info()
            if self.mode in ["new", "special"]:
                extra_info = True
            else:
                extra_info = False

            if extra_info:
                tags: List[str] = [tag['tag_name'] for tag in await v.get_tags()]
        
                if info['duration'] <= self.config.MIN_VIDEO_DURATION:
                    logger.info(f"跳过短视频： {bvid}")
                    return None
                
            logger.info(f"获取视频信息： {bvid}")
            return VideoInfo(
                **existing_data if self.mode == "old" else {
                'name': self.clean_tags(info['title']),
                'bvid': bvid,
                'author': info['owner']['name'],
                'synthesizer': "",
                'vocal': "",
                'type': "",
                'copyright': info['copyright'],
                'pubdate': datetime.fromtimestamp(info['pubdate']).strftime('%Y-%m-%d %H:%M:%S'),
                },
                title=self.clean_tags(info['title']),
                uploader=info['owner']['name'],
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
            logger.error(f"爬取 {bvid} 时出错: {str(e)}")
            raise
    
    def calculate_failed_mask(self, update_df: pd.DataFrame, census_mode: bool) -> pd.Series:
        if census_mode:
            return ~self.songs['bvid'].isin(update_df['bvid'])
        mask = (
            (self.songs['streak'] < self.config.STREAK_THRESHOLD) & 
            ~self.songs['bvid'].isin(update_df['bvid'])
        )
        return mask

    def process_streaks(self, old_views: pd.Series, updated_ids: pd.Index, census_mode: bool):
        for bvid in updated_ids:
            new_view = self.songs.at[bvid, 'view']
            old_view = old_views.get(bvid, new_view)
            actual_incr = new_view - old_view
            
            current_streak = self.songs.at[bvid, 'streak']
            threshold = self.calculate_threshold(current_streak, census_mode)
            
            condition = (new_view < self.config.MIN_TOTAL_VIEW) and (actual_incr < threshold)
            self.songs.at[bvid, 'streak'] = current_streak + 1 if condition else 0
        
        if not census_mode:
            unprocessed = ~self.songs.index.isin(updated_ids) & ~self.songs['is_failed']
            self.songs.loc[unprocessed, 'streak'] += 1
        
        self.songs.loc[self.songs['is_failed'], 'streak'] = 0

    def calculate_threshold(self, current_streak: int, census_mode: bool) -> int:
        if not census_mode:
            return self.config.BASE_THRESHOLD
        gap = min(7, max(0, current_streak - self.config.STREAK_THRESHOLD))
        return self.config.BASE_THRESHOLD * (gap + 1)
    
    # =================== 工作步骤函数 ===================
    async def get_all_bvids(self) -> List[str]:
        """使用搜索和分区两种方式"""
        bvids = set(await self.get_video_list_by_search(time_filtering=self.mode == "new"))
        if self.mode == "new":
            bvids.update(await self.get_video_list_by_zone())
        return list(bvids)
    def update_recorded_songs(self, videos: List[VideoInfo], census_mode: bool):
        """更新收录曲目表"""
        update_df = pd.DataFrame([{
            'bvid': video.bvid,
            'title': video.title,
            'view': video.view,
            'uploader': video.uploader,
            'image_url': video.image_url,
        } for video in videos])
        old_views = self.songs.set_index('bvid')['view']
        self.songs['is_failed'] = self.calculate_failed_mask(update_df, census_mode)
        self.songs = self.songs.set_index('bvid')
        update_df = update_df.set_index('bvid')
        self.songs.update(update_df)
        self.process_streaks(old_views, update_df.index, census_mode)
        self.songs = self.songs.reset_index()
        self.songs = self.songs.reset_index().sort_values(['is_failed', 'view'], ascending=[False, False]).drop('is_failed', axis=1)
        save_to_excel(self.songs, "收录曲目.xlsx", usecols=json.load(Path('config/usecols.json').open(encoding='utf-8'))["columns"]["record"])

    async def get_video_details(self, bvids: List[str]) -> List[VideoInfo]:
        """获取列表中所有视频详细信息"""
        sem = asyncio.Semaphore(self.config.SEMAPHORE_LIMIT)
        async def sem_fetch(bvid: str) -> Optional[VideoInfo]:
            async with sem:
                if self.proxy:
                    request_settings.set_proxy(self.proxy.proxy_server)  
                result = await self.retry_handler.retry_async(self.fetch_video_detail, bvid)
                await asyncio.sleep(self.config.SLEEP_TIME)
                return result

        results = await asyncio.gather(*[sem_fetch(bvid) for bvid in bvids], return_exceptions=True)
        return [r for r in results if isinstance(r, VideoInfo)]


    #   =================== 完整工作流使用的函数 =====================
    async def process_new_songs(self) -> List[Dict[str, Any]]:
        """抓取新曲数据"""
        logger.info("开始获取新曲数据")
        bvids = await self.get_all_bvids()
        logger.info(f"一共有 {len(bvids)} 个 bvid")
        videos = await self.get_video_details(bvids)
        return [asdict(video) for video in videos]
    
    async def process_old_songs(self) -> List[Dict[str, Any]]:
        """抓取旧曲数据"""
        logger.info("开始获取旧曲数据")
        census_mode = self.is_census_day()
        if census_mode:
            bvids = self.songs['bvid'].tolist()
            logger.info(f"普查模式：处理全部 {len(bvids)} 个视频")
        else:
            mask = self.songs['streak'] < self.config.STREAK_THRESHOLD
            bvids = self.songs.loc[mask, 'bvid'].tolist()
            logger.info(f"常规模式：处理 {len(bvids)} 个视频")

        videos = await self.get_video_details(bvids)
        self.update_recorded_songs(videos, census_mode)
        return [asdict(v) for v in videos]
    
    # =================== 单独使用 =====================
    async def save_to_excel(self, videos: List[Dict[str, Any]], usecols: Optional[List[str]] = None) -> None:
        """导出数据"""
        df = pd.DataFrame(videos)
        df = df.sort_values(by='view', ascending=False)
        save_to_excel(df, self.filename, usecols=usecols)
