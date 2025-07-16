# src/bilibili_scraper.py
# B站爬虫模块：负责视频数据的抓取、处理和导出
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
    """
    视频信息数据类
    存储从B站API和已有数据库获取的视频详细信息
    """
    title: str              # 视频自带标题
    bvid: str               # bvid
    aid: str                # aid
    name: str               # 曲目名称
    author: str             # 作者名称
    uploader: str = ""      # 视频上传者名称
    copyright: int = 0      # 版权类型
    synthesizer: str = ""   # 引擎
    vocal: str = ""         # 歌手
    type: str = ""          # 视频类型
    pubdate: str = ""       # 发布时间
    duration: str = ""      # 视频时长
    page: int = 0           # 分P数
    view: int = 0           # 播放
    favorite: int = 0       # 收藏
    coin: int = 0           # 硬币
    like: int = 0           # 点赞
    image_url: str = ""     # 封面URL
    tags: Optional[str] = None # 视频标签
    description: Optional[str] = None # 视频简介
    streak : int = 0        # 人工变量：连续未达标次数

@dataclass    
class VideoInvalidException(Exception):
    """自定义异常，用于表示视频已失效或无法访问。"""
    def __init__(self, message):
        super().__init__(message)
        self.message = message

@dataclass
class SearchOptions:
    """B站搜索参数配置类"""
    search_type: search.SearchObjectType = search.SearchObjectType.VIDEO
    order_type: search.OrderVideo = search.OrderVideo.PUBDATE
    video_zone_type: Optional[List[int]] = None
    order_sort: Optional[int] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None
    page_size: Optional[int] = 30

@dataclass
class Config:
    """爬虫全局配置"""
    KEYWORDS: List[str] = field(default_factory=list)   # 搜索关键词列表
    HEADERS: List[str] = field(default_factory=lambda: [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.67 Safari/537.36',
    ])
    MAX_RETRIES: int = 5                # 最大重试次数
    SEMAPHORE_LIMIT: int = 5            # 并发请求限制
    MIN_VIDEO_DURATION: int = 20        # 最小视频时长(秒)
    SLEEP_TIME: float = 0.8             # 请求间隔时间(秒)
    OUTPUT_DIR: Path = Path("新曲数据")  # 输出目录
    NAME: Optional[str] = None          # 特刊名称(如果有)
    STREAK_THRESHOLD : int = 7          # 连续未达标次数阈值
    MIN_TOTAL_VIEW: int = 10000         # 脱离阈值判定的播放下限
    BASE_THRESHOLD: int = 100           # 日增播放判定阈值

    @staticmethod
    def load_keywords(file_path: str = "keywords.json") -> List[str]:
        """从JSON文件加载搜索关键词"""
        with open(Path(file_path), "r", encoding="utf-8") as f:
            keywords = json.load(f)
        return keywords
    
class BilibiliScraper:
    """
    B站视频爬虫类
    
    主要功能:
    1. 搜索并获取视频列表
    2. 获取视频详细信息
    3. 更新已收录视频数据
    4. 导出数据到Excel
    
    工作模式:
    - new: 抓取最近发布的新视频
    - old: 更新已收录视频的数据
    - special: 特殊抓取模式
    
    代码结构:
    1. 基础设施
       - 初始化配置
       - 工具方法
       - 会话管理
       
    2. 数据处理
       - 数据清理
       - 格式转换
       - 阈值计算
       
    3. API交互
       - 视频搜索
       - 信息获取
       - 批量处理
       
    4. 核心业务
       - 视频状态追踪
       - 数据更新
       - 导出功能
       
    5. 主工作流
       - 新曲处理
       - 旧曲更新
    """
    # =================== 1. 基础设施 ===================
    
    # 1.1 类属性
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
        初始化爬虫实例
        
        Args:
            mode: 工作模式
            input_file: 收录曲目文件(old模式需要)
            days: 新曲模式下往前查找的天数
            config: 全局配置
            search_options: 搜索配置
            proxy: 代理配置
        """
        self.mode = mode
        self.config = config
        self.config.OUTPUT_DIR.mkdir(exist_ok=True)
        self.search_options = search_options
        self.session = None
        self.sem = asyncio.Semaphore(self.config.SEMAPHORE_LIMIT)
        self.retry_handler = RetryHandler(Config.MAX_RETRIES, Config.SLEEP_TIME)

        # 根据模式初始化
        if self.mode == "new":
            # 新曲模式：设置输出文件名和时间范围
            self.filename = self.config.OUTPUT_DIR / f"新曲{self.today.strftime('%Y%m%d')}.xlsx"
            self.start_time = self.today - timedelta(days=days)
        elif self.mode == "old":
            # 旧曲模式：读取收录曲目表
            self.filename = self.config.OUTPUT_DIR / f"{self.today.strftime('%Y%m%d')}.xlsx"
            self.songs = pd.read_excel(input_file)
            if 'streak' not in self.songs.columns:
                self.songs['streak'] = 0
            
            if 'aid' not in self.songs.columns:
                self.songs['aid'] = '' 
            else:
                self.songs['aid'] = self.songs['aid'].astype(str).str.replace(r'\.0$', '', regex=True)
        
        elif self.mode == "special":
            # 特刊模式：使用指定文件名
            self.filename = self.config.OUTPUT_DIR / f"{self.config.NAME}.xlsx"

        # 代理配置
        if proxy:
            request_settings.set_proxy(proxy.proxy_server)
            self.proxy = proxy
            self.config.SLEEP_TIME = 0.5
            self.config.SEMAPHORE_LIMIT = 20
            
    # 1.2 会话管理
    async def get_session(self):
        """
        获取或创建aiohttp会话
        实现会话复用，提高性能
        """
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close_session(self):
        """
        关闭aiohttp会话
        确保资源正确释放
        """
        if self.session:
            await self.session.close()
            self.session = None

    # 1.3 工具函数
    @staticmethod
    def clean_tags(text: str) -> str:
        """清理HTML标签和特殊字符"""
        text = re.sub(r'<.*?>', '', text)
        return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F\u200B-\u200F\u2028-\u202F\u205F-\u206F\uFEFF\uFFFE-\uFFFF]','', text)

    @staticmethod
    def convert_duration(duration: int) -> str:
        """
        转换视频时长为人类可读格式
        由于b站进一法处理时长，这里直接使用秒数

        Args:
            duration: 视频时长(秒)
        
        Returns:
            str: 格式化的时长字符串(例如: "3分20秒")
        """
        minutes, seconds = divmod(duration - 1, 60)
        return f'{minutes}分{seconds}秒' if minutes > 0 else f'{seconds}秒'

    def is_census_day(self) -> bool:
        """判断是否为普查日（周六或每月1日）"""
        return (self.today.weekday() == 5) or (self.today.day == 1)
    
    # =================== 2. 数据处理 ===================
    
    # 2.1 阈值计算
    def calculate_dynamic_threshold(self, streak: int) -> int:
        """
        计算动态播放增长阈值
        
        算法:
        1. 如果连续未达标次数<=阈值，使用基础阈值
        2. 否则根据超出阈值的天数线性增加要求
        
        Args:
            streak: 连续未达标次数
            
        Returns:
            int: 计算得到的播放增长阈值
        """
        if streak <= self.config.STREAK_THRESHOLD:
            return self.config.BASE_THRESHOLD
        gap_days = streak - self.config.STREAK_THRESHOLD
        return self.config.BASE_THRESHOLD * (gap_days + 1)
    
    def calculate_threshold(self, current_streak: int, census_mode: bool) -> int:
        """
        计算单个视频的播放增长阈值
        
        阈值计算规则:
        1. 非普查模式：统一使用基础阈值(BASE_THRESHOLD)
        2. 普查模式：动态计算
           - streak未超过阈值时使用基础阈值
           - 超过阈值后，每多1天未达标，要求增加1倍基础阈值
           - 最多增加到原阈值的8倍
        
        参数:
            current_streak (int): 当前连续未达标次数
            census_mode (bool): 是否为普查模式
            
        返回:
            int: 计算得到的播放增长阈值
        """
        if not census_mode:
            return self.config.BASE_THRESHOLD
        gap = min(7, max(0, current_streak - self.config.STREAK_THRESHOLD))
        return self.config.BASE_THRESHOLD * (gap + 1)
    
    # 2.2 状态处理
    def calculate_failed_mask(self, update_df: pd.DataFrame, census_mode: bool) -> pd.Series:
        """
        计算视频失效状态掩码
        
        逻辑:
        - 普查模式：所有无法更新数据的视频标记为失效
        - 常规模式：未达到阈值且无法更新的视频标记为失效
        
        Args:
            update_df: 已更新的视频数据
            census_mode: 是否为普查模式
        
        Returns:
            pd.Series: 布尔掩码，True表示视频失效
        """
        if census_mode:
            return ~self.songs['bvid'].isin(update_df['bvid'])
        mask = (
            (self.songs['streak'] < self.config.STREAK_THRESHOLD) & 
            ~self.songs['bvid'].isin(update_df['bvid'])
        )
        return mask

    def process_streaks(self, old_views: pd.Series, updated_ids: pd.Index, census_mode: bool):
        """
        处理视频的连续未达标状态
        
        工作流程:
        1. 对于每个已更新数据的视频:
           - 比较新旧播放量计算增长值
           - 根据当前streak和模式计算阈值
           - 根据增长值和总播放量判断是否达标
           - 更新streak计数
           
        2. 非普查模式下处理未更新的视频:
           - 未失效且未更新的视频streak+1
           
        3. 处理失效视频:
           - 所有失效视频的streak重置为0
        
        参数:
            old_views (pd.Series): 更新前的播放量数据，索引为bvid
            updated_ids (pd.Index): 已成功更新数据的视频bvid索引
            census_mode (bool): 是否为普查模式
        """
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

    # =================== 3. API交互 ===================
    
    # 3.1 基础请求
    async def fetch_data(self, url: str) -> Optional[Dict]:
        """
        通用异步HTTP GET请求函数
        
        特性:
        1. 随机选择User-Agent
        2. 自动处理JSON响应
        3. 仅返回成功(200)的响应
        
        Args:
            url: 请求URL
            
        Returns:
            Dict: JSON响应数据
            None: 请求失败
        """
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': random.choice(self.config.HEADERS)}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
        return None

    def search_by_type(self, keyword, page, search_options: SearchOptions):
        """
        封装bilibili-api的搜索功能
        
        Args:
            keyword: 搜索关键词
            page: 页码
            search_options: 搜索参数配置
        """
        return search.search_by_type(
            keyword,
            search_type= search_options.search_type,
            order_type= search_options.order_type,
            video_zone_type= search_options.video_zone_type,
            time_start= search_options.time_start,
            time_end= search_options.time_end,
            page=page,
            page_size= search_options.page_size
        )
    
    # 3.2 视频搜索
    async def get_video_list_by_zone(self, rid: int = 30, ps: int = 50) -> List[str]:
        """
        从B站分区API获取视频列表
        
        工作流程:
        1. 按页遍历分区视频
        2. 过滤出指定时间范围内的视频
        3. 处理重试和异常
        4. 去重返回bvid列表
        
        Args:
            rid: 分区ID(默认30=VOCALOID)
            ps: 每页视频数
            
        Returns:
            List[str]: 去重后的bvid列表
        """
        bvids: List[str] = []
        page = 1
        try:
            while True:
                # 设置代理(如果有)
                if self.proxy:
                    request_settings.set_proxy(self.proxy.proxy_server) 
                url = f"https://api.bilibili.com/x/web-interface/newlist?rid={rid}&ps={ps}&pn={page}"
                
                jsondata = await self.retry_handler.retry_async(self.fetch_data, url)
                if (jsondata):
                    video_list = jsondata['data']['archives']
                    # 过滤出指定时间范围的视频
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
            return list(set(bvids))

        except Exception as e:
            logger.error('搜索分区视频时出错：', e)
            return list(set(bvids))

    async def get_video_list_by_search(self, time_filtering: bool = False) -> List[str]:
        """
        通过搜索API获取视频列表
        
        工作流程:
        1. 遍历每个目标分区
        2. 在分区内搜索视频
        3. 合并去重
        
        Args:
            time_filtering: 是否按发布时间过滤
            
        Returns:
            List[str]: 所有找到的视频bvid列表
        """
        all_bvids = set()
        for zone in self.search_options.video_zone_type:
            bvids = await self.get_video_list_by_search_for_zone(zone, time_filtering=time_filtering)
            all_bvids.update(bvids)
            await asyncio.sleep(self.config.SLEEP_TIME)

        return list(all_bvids)
    
    async def get_video_list_by_search_for_zone(self, zone: Optional[int], time_filtering: bool = False) -> List[str]:
        """
        在指定分区内搜索视频
        
        实现细节:
        1. 批量处理关键词(每批3个)
        2. 使用信号量控制并发
        3. 支持分页和增量获取
        4. 自动处理代理设置
        
        参数:
            zone: 目标分区ID
            time_filtering: 是否按时间过滤结果
            
        返回:
            List[str]: 该分区内找到的所有bvid
        """
        keywords = self.config.KEYWORDS[:]
        bvids = []
        batch_size = 3
        keyword_pages = {keyword: 1 for keyword in keywords} 
        active_keywords = keywords[:] # 激发关键词列表

        while active_keywords:
            current_batch = active_keywords[:batch_size]
            logger.info(f'[分区 {zone}] 处理关键词批次: {current_batch}')

            async def sem_fetch(keyword: str) -> Dict:
                """
                并发搜索处理函数
                """
                async with self.sem:
                    if self.proxy:
                        request_settings.set_proxy(self.proxy.proxy_server)
                    # 构建搜索参数
                    search_opts = SearchOptions(
                        search_type=self.search_options.search_type,
                        order_type=self.search_options.order_type,
                        video_zone_type=zone,
                        time_start=self.search_options.time_start,
                        time_end=self.search_options.time_end
                    )
                    # 执行搜索并处理重试
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
                    
                    # 处理搜索结果
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
                    
                    await asyncio.sleep(self.config.SLEEP_TIME * 2)
                    return {'end': False, 'keyword': keyword, 'bvids': temp_bvids}

            # 并发执行当前批次的搜索
            tasks = [sem_fetch(keyword) for keyword in current_batch]
            results = await asyncio.gather(*tasks)

            # 处理搜索结果
            for result in results:
                keyword = result['keyword']
                bvids.extend(result['bvids'])
                if result['end']:
                    # 该关键词搜索完成，从激发列表中移除
                    if keyword in active_keywords:
                        active_keywords.remove(keyword)
                else:
                    # 继续搜索下一页
                    keyword_pages[keyword] += 1
            # 更新激发关键词列表
            if current_batch:
                remaining_keywords = [k for k in active_keywords if k not in current_batch]
                active_keywords = remaining_keywords + [k for k in current_batch if k in active_keywords]

            await asyncio.sleep(self.config.SLEEP_TIME)

        return list(set(bvids))

    # 3.3 视频信息获取
    async def fetch_video_detail(self, bvid: str) -> Optional[VideoInfo]:
        """获取单个视频的详细信息"""
        try:
            existing_data = {}
            if self.mode == "old":
                song_data = self.songs[self.songs['bvid'] == bvid].iloc[0]
                existing_data = {
                    'name':         song_data['name'],
                    'bvid':         bvid,
                    'aid' :         song_data['aid'],
                    'author':       song_data['author'],
                    'synthesizer':  song_data['synthesizer'],
                    'vocal':        song_data['vocal'],
                    'copyright':    song_data['copyright'],
                    'type':         song_data['type'],
                    'pubdate':      song_data['pubdate'],
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
                'aid' : str(info['aid']),
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
        except Exception:
            raise
    
    async def _get_batch_details_by_aid(self, aids: List[int]) -> Dict[int, Dict]:
        """
        使用B站medialist接口批量获取视频信息
        
        技术细节:
        1. 每批处理50个aid
        2. 使用会话复用提高性能
        3. 自动重试和错误处理
        4. 支持超时控制
        
        Args:
            aids: 视频aid列表
        
        Returns:
            Dict[int, Dict]: aid到视频信息的映射
        """
        BATCH_SIZE = 50
        all_stats = {}
        session = await self.get_session()

        # 分批处理
        for i in range(0, len(aids), BATCH_SIZE):
            batch_aids = aids[i:i + BATCH_SIZE]
            resources_str = ",".join([f"{aid}:2" for aid in batch_aids])
            url = f"https://api.bilibili.com/medialist/gateway/base/resource/infos?resources={resources_str}"
            
            logger.info(f"正在通过 medialist 接口处理批次 {i//BATCH_SIZE + 1}，包含 {len(batch_aids)} 个视频...")

            try:
                async with session.get(url, headers={'User-Agent': random.choice(self.config.HEADERS)}, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == 0 and data.get('data'):
                            for item in data['data']:
                                all_stats[item['id']] = item
                        else:
                            logger.warning(f"API 返回错误或无数据，批次：{batch_aids}, 响应: {data.get('message', 'N/A')}")
                    else:
                        logger.error(f"请求批次失败，HTTP状态码: {response.status}, 批次: {batch_aids}")
                
                await asyncio.sleep(self.config.SLEEP_TIME)

            except Exception as e:
                logger.error(f"处理批次时发生异常: {e}, 批次: {batch_aids}")

        return all_stats

    # =================== 4. 核心业务 ===================
    
    # 4.1 视频列表获取
    async def get_all_bvids(self) -> List[str]:
        """
        综合搜索和分区两种方式获取目标视频的bvid列表
        
        工作流程:
        1. 使用搜索API获取视频
           - 新曲模式：仅获取指定时间范围内的视频
           - 特刊模式：获取所有匹配的视频
           
        2. 新曲模式额外处理:
           - 从分区API获取最新视频
           - 合并两种方式的结果
           - 去重处理
        
        返回:
            List[str]: 去重后的bvid列表
        """
        bvids = set(await self.get_video_list_by_search(time_filtering = self.mode == "new"))
        if self.mode == "new":
            bvids.update(await self.get_video_list_by_zone())
        return list(set(bvids))
    
    async def get_video_details(self, bvids: List[str]) -> List[VideoInfo]:
        """
        并发获取多个视频的详细信息
        
        实现细节:
        1. 自动重试失败的请求
        2. 异步并发处理多个视频
        3. 过滤掉请求失败的结果
        
        参数:
            bvids (List[str]): 需要获取详情的视频bvid列表
            
        返回:
            List[VideoInfo]: 成功获取的视频信息列表
        """
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

    # 4.2 数据更新
    def update_recorded_songs(self, videos: List[VideoInfo], census_mode: bool):
        """
        更新已收录曲目的数据
        
        工作流程:
        1. 数据预处理
           - 将新获取的视频信息转换为DataFrame
           - 保存更新前的播放量数据
           
        2. 更新步骤
           - 计算视频失效状态
           - 更新视频数据
           - 处理连续未达标状态
           
        3. 数据后处理
           - 重置索引
           - 按失效状态和播放量排序
           - 导出更新后的收录曲目表
           
        参数:
            videos (List[VideoInfo]): 需要更新的视频信息列表
            census_mode (bool): 是否为普查模式
        """
        update_df = pd.DataFrame([{
            'bvid': video.bvid,
            'aid': video.aid,
            'title': video.title,
            'view': video.view,
            'uploader': video.uploader,
            'copyright': video.copyright,
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

    async def save_to_excel(self, videos: List[Dict[str, Any]], usecols: Optional[List[str]] = None) -> None:
        """导出数据"""
        df = pd.DataFrame(videos)
        df = df.sort_values(by='view', ascending=False)
        save_to_excel(df, self.filename, usecols=usecols)

    # =================== 5. 主工作流 ===================
    
    async def process_new_songs(self) -> List[Dict[str, Any]]:
        """
        处理新发布的歌曲数据的主函数
        
        工作流程:
        1. 获取目标视频列表
           - 通过搜索API获取
           - 通过分区API获取
           - 合并去重
           
        2. 获取视频详细信息
           - 并发请求视频数据
           - 过滤无效结果
           
        3. 数据转换
           - 将VideoInfo对象转换为字典格式
        
        返回:
            List[Dict[str, Any]]: 新发布视频的详细信息列表
        """
        logger.info("开始获取新曲数据")
        bvids = await self.get_all_bvids()
        logger.info(f"一共有 {len(bvids)} 个 bvid")
        videos = await self.get_video_details(bvids)
        return [asdict(video) for video in videos]
    
    async def process_old_songs(self) -> List[Dict[str, Any]]:
        """
        处理已收录歌曲数据的主函数
        
        工作流程:
        1. 确定处理模式和范围
           - 普查模式：处理所有视频
           - 常规模式：只处理未达到streak阈值的视频
           
        2. 批量获取视频数据
           - 使用medialist接口批量获取
           - 处理API返回的结果
           
        3. 数据更新
           - 构建VideoInfo对象
           - 更新收录曲目表
           - 处理视频状态
           
        4. 数据转换
           - 将更新后的数据转换为字典格式
        
        返回:
            List[Dict[str, Any]]: 已更新视频的详细信息列表
        """
        logger.info("开始获取旧曲数据")
        census_mode = self.is_census_day()

        if census_mode:
            songs_to_process_df = self.songs
            logger.info(f"普查模式：准备处理全部 {len(songs_to_process_df)} 个视频")
        else:
            mask = self.songs['streak'] < self.config.STREAK_THRESHOLD
            songs_to_process_df = self.songs.loc[mask]
            logger.info(f"常规模式：准备处理 {len(songs_to_process_df)} 个视频")

        if songs_to_process_df.empty:
            logger.info("没有需要处理的旧曲，任务结束。")
            return []
        
        aids_to_fetch = [int(aid) for aid in songs_to_process_df['aid'] if aid and aid.isdigit()]
        batch_results = await self._get_batch_details_by_aid(aids_to_fetch)

        videos = []
        for aid, stats in batch_results.items():
            try:
                song_data = songs_to_process_df[songs_to_process_df['aid'] == str(aid)].iloc[0]
                if stats.get('title') == "已失效视频":
                    raise VideoInvalidException(f"视频已失效")
                
                video_info = VideoInfo(
                    aid=aid,
                    bvid=song_data['bvid'],
                    name=song_data['name'],
                    author=song_data['author'],
                    synthesizer=song_data['synthesizer'],
                    vocal=song_data['vocal'],
                    copyright=song_data['copyright'] if song_data['copyright'] in [3, 4] else stats.get('copyright', 1),
                    type=song_data['type'],
                    pubdate=datetime.fromtimestamp(stats.get('ctime', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                    title=self.clean_tags(stats.get('title', song_data['name'])),
                    uploader=stats.get('upper', {}).get('name', {}),
                    duration=self.convert_duration(stats.get('duration', 0)),
                    page=stats.get('page', 1),
                    view=stats.get('cnt_info', {}).get('play', 0),
                    favorite=stats.get('cnt_info', {}).get('collect', 0),
                    coin=stats.get('cnt_info', {}).get('coin', 0),
                    like=stats.get('cnt_info', {}).get('thumb_up', 0),
                    image_url=stats.get('cover', ''),
                    tags=None,
                    description=None
                )
                videos.append(video_info)
            except Exception as e:
                logger.error(f"处理 aid {aid} 的批量结果时出错: {e}")

        self.update_recorded_songs(videos, census_mode)
        return [asdict(v) for v in videos]
