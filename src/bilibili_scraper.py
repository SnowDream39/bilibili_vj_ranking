# src/bilibili_scraper.py
# B站爬虫模块：负责视频数据的抓取、处理和导出
import asyncio
import aiohttp
import pandas as pd
from bilibili_api import request_settings, search
from datetime import datetime, timedelta
import random
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Literal, Any, Set, Union
from pathlib import Path
import json
from utils.logger import logger
from utils.io_utils import save_to_excel
from utils.proxy import Proxy 
from utils.retry_handler import RetryHandler
from utils.formatters import clean_tags, convert_duration
from utils.calculator import calculate_threshold, calculate_failed_mask

@dataclass
class VideoInfo:
    """存储从B站API和已有数据库获取的视频详细信息。"""
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
    streak : int = 0        # 连续未达标次数

@dataclass    
class VideoInvalidException(Exception):
    """自定义异常，用于表示视频已失效或无法访问。"""
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

@dataclass
class SearchOptions:
    """B站搜索参数配置类"""
    search_type: search.SearchObjectType = search.SearchObjectType.VIDEO
    order_type: search.OrderVideo = search.OrderVideo.PUBDATE
    video_zone_type: Optional[int] = None
    order_sort: Optional[int] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None
    page_size: Optional[int] = 30

@dataclass
class SearchRestrictions:
    """B站搜索过滤条件配置类"""
    min_favorite: Optional[int] = None
    min_view:Optional[int] = None

@dataclass
class Config:
    """爬虫全局配置"""
    KEYWORDS: List[str] = field(default_factory=list)  
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
    B站视频爬虫，用于根据不同模式（新曲、旧曲、特刊）抓取、处理和更新视频数据。
    """
    # 确定今天的日期，如果当前时间超过晚上11点，则算作第二天
    today: datetime = (datetime.now() + timedelta(days=1) if datetime.now().hour >= 23 else datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    search_options: list[SearchOptions] = [SearchOptions()]
    search_restrictions: SearchRestrictions | None
    proxy = None

    def __init__(self, 
                 mode: Literal["new", "old", "special"], 
                 input_file: Union[str, Path, None] = None, 
                 days: int = 2,
                 config: Config = Config(), 
                 search_options: list[SearchOptions] = [SearchOptions()],
                 search_restrictions: SearchRestrictions | None = None,
                 proxy: Optional[Proxy] = None,
                ):
        """
        初始化爬虫实例。

        Args:
            mode: 工作模式 ('new', 'old', 'special')。
            input_file: 收录曲目文件路径 (仅 'old' 模式需要)。
            days: 'new' 模式下往前查找的天数。
            config: 全局配置对象。
            search_options: 搜索参数配置对象。
            proxy: 代理配置对象。
        """
        self.mode: Literal["new", "old", "special", None] = mode
        self.config = config
        self.config.OUTPUT_DIR.mkdir(exist_ok=True)
        self.search_options = search_options
        self.search_restrictions = search_restrictions
        self.session = None
        self.sem = asyncio.Semaphore(self.config.SEMAPHORE_LIMIT)
        self.retry_handler = RetryHandler(Config.MAX_RETRIES, Config.SLEEP_TIME)

        # 根据不同模式进行初始化
        if self.mode == "new":
            self.filename = self.config.OUTPUT_DIR / f"新曲{self.today.strftime('%Y%m%d')}.xlsx"
            self.start_time = self.today - timedelta(days=days)
        elif self.mode == "old":
            self.filename = self.config.OUTPUT_DIR / f"{self.today.strftime('%Y%m%d')}.xlsx"
            self.songs = pd.read_excel(input_file)
            
            if 'streak' not in self.songs.columns:
                self.songs['streak'] = 0
            
            if 'aid' not in self.songs.columns:
                self.songs['aid'] = '' 
            else:
                self.songs['aid'] = self.songs['aid'].astype(str).str.replace(r'\.0$', '', regex=True)
        
        elif self.mode == "special":
            # 特刊模式：根据配置中的特刊名称设置文件名
            self.filename = self.config.OUTPUT_DIR / f"{self.config.NAME}.xlsx"

        # 代理配置
        if proxy:
            request_settings.set_proxy(proxy.proxy_server)
            self.proxy = proxy
            self.config.SLEEP_TIME = 0.5
            self.config.SEMAPHORE_LIMIT = 20
            
    async def get_session(self):
        """获取或创建 aiohttp 会话以实现复用。"""
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close_session(self):
        """关闭 aiohttp 会话以释放资源。"""
        if self.session:
            await self.session.close()
            self.session = None

    def is_census_day(self) -> bool:
        """判断是否为普查日（周六或每月1日）"""
        # 普查日需要处理所有旧曲，而非普查日只处理未连续掉出阈值的曲目
        return (self.today.weekday() == 5) or (self.today.day == 1)

    def process_streaks(self, old_views: pd.Series, updated_ids: pd.Index, census_mode: bool):
        """
        处理并更新所有相关视频的连续未达标（streak）计数。
        
        Args:
            old_views: 更新前的播放量数据，以bvid为索引。
            updated_ids: 已成功更新数据的视频bvid索引。
            census_mode: 是否为普查模式。
        """
         # 遍历所有成功更新的视频
        for bvid in updated_ids:
            # 获取新旧播放量，计算实际增量
            new_view = self.songs.at[bvid, 'view']
            old_view = old_views.get(bvid, new_view)
            actual_incr = new_view - old_view
            
            # 获取当前连续未达标次数并计算本次的阈值
            current_streak = self.songs.at[bvid, 'streak']
            threshold = calculate_threshold(current_streak, census_mode, self.config.BASE_THRESHOLD, self.config.STREAK_THRESHOLD)
            
            # 判断是否达标：总播放量低于下限 且 日增播放低于动态阈值
            condition = (new_view < self.config.MIN_TOTAL_VIEW) and (actual_incr < threshold)
            # 如果未达标，streak+1；否则清零
            self.songs.at[bvid, 'streak'] = current_streak + 1 if condition else 0
        
        # 在非普查模式下，对于超过阈值后不再更新数据的视频，也视为未达标，streak+1
        if not census_mode:
            unprocessed = ~self.songs.index.isin(updated_ids) & ~self.songs['is_failed']
            self.songs.loc[unprocessed, 'streak'] += 1
        
        # 对于已标记为失效的视频，其streak计数重置为0
        self.songs.loc[self.songs['is_failed'], 'streak'] = 0

    async def fetch_data(self, url: str) -> Optional[Dict[str, Any]]:
        """
        通用的异步HTTP GET请求函数，支持随机User-Agent和自动JSON解析。
        
        Args:
            url: 请求的URL。
            
        Returns:
            成功时的JSON响应数据字典，否则为None。
        """
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': random.choice(self.config.HEADERS)}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
        return None

    def search_by_type(self, keyword: str, page: int, search_options: SearchOptions):
        """
        封装 bilibili-api 的搜索功能。
        
        Args:
            keyword: 搜索关键词。
            page: 页码。
            search_options: 搜索参数配置。
        """
        return search.search_by_type(
            keyword,
            search_type= search_options.search_type,
            order_type= search_options.order_type,
            video_zone_type= search_options.video_zone_type,
            time_start= search_options.time_start,
            time_end= search_options.time_end,
            page=page,
            page_size= search_options.page_size or 30
        )

    async def get_video_list_by_zone(self, rid: int = 30, ps: int = 50) -> List[str]:
        """
        通过B站分区API获取指定时间范围内的视频列表。

        Args:
            rid: 分区ID (默认为30, VOCALOID)。
            ps: 每页视频数。
            
        Returns:
            去重后的aid字符串列表。
        """
        aids: List[str] = []
        page = 1
        try:
            # 循环翻页直到没有新视频或不满足时间条件
            while True:
                if self.proxy:
                    request_settings.set_proxy(self.proxy.proxy_server) 
                url = f"https://api.bilibili.com/x/web-interface/newlist?rid={rid}&ps={ps}&pn={page}"
                
                # 使用带重试的处理器发起请求
                jsondata = await self.retry_handler.retry_async(self.fetch_data, url)
                if (jsondata):
                    video_list = jsondata['data']['archives']
                    # 过滤出发布时间在指定范围内的视频
                    recent_videos = [
                        video for video in video_list
                        if datetime.fromtimestamp(video['pubdate']) > self.start_time
                    ]
                    logger.info(f"获取分区最新： {rid}，第 {page} 页")
                    # 如果当前页没有符合时间条件的视频，则停止翻页
                    if not recent_videos:
                        break
                    # 将符合条件的视频aid加入列表
                    aids.extend(str(video['aid']) for video in recent_videos)
                    page += 1
                    await asyncio.sleep(self.config.SLEEP_TIME)
                else:
                    raise Exception("获取数据失败")
            # 返回去重后的aid列表
            return list(set(aids))

        except Exception as e:
            logger.error('搜索分区视频时出错：', e)
            # 即使出错也返回已获取到的部分数据
            return list(set(aids))

    async def get_video_list_by_search_for_zone(self, search_options: SearchOptions) -> List[str]:
        """
        在指定分区内，通过批量和并发的方式搜索视频。
        
        Args:
            zone: 目标分区ID。
            time_filtering: 是否按时间过滤结果。
            
        Returns:
            该分区内找到的所有aid字符串列表。
        """
        keywords = self.config.KEYWORDS[:]
        aids = []
        batch_size = 3  # 每次并发处理的关键词数量
        keyword_pages = {keyword: 1 for keyword in keywords} # 记录每个关键词的当前搜索页码
        active_keywords = keywords[:] # 维护一个仍在搜索中的关键词列表

        while active_keywords:
            # 从活动关键词列表中取出一个批次进行处理
            current_batch = active_keywords[:batch_size]
            logger.info(f'[分区 {search_options.video_zone_type}] 处理关键词批次: {current_batch}')

            # 使用信号量控制并发数量
            async def sem_fetch(keyword: str) -> Dict[str, Any]:
                """并发搜索处理函数"""
                async with self.sem:
                    if self.proxy:
                        request_settings.set_proxy(self.proxy.proxy_server)
                    
                    # 使用带重试的处理器执行搜索
                    result = await self.retry_handler.retry_async(
                        self.search_by_type, 
                        keyword, 
                        keyword_pages[keyword], 
                        search_options
                    )
                    
                    if not result or 'result' not in result:
                        return {'end': True, 'keyword': keyword, 'aids': []}
                    
                    videos = result.get('result', [])
                    end = not videos or len(videos) < (search_options.page_size or 30)

                    temp_aids = []
                    for item in videos:

                        if self.search_restrictions:
                            # 如果满足 search_restrictions 设置的条件，立即结束
                            if self.search_restrictions.min_favorite and item['favorites'] < self.search_restrictions.min_favorite:
                                return {'end': True, 'keyword': keyword, 'aids': temp_aids}
                            
                            if self.search_restrictions.min_view and item['play'] < self.search_restrictions.min_view:
                                return {'end': True, 'keyword': keyword, 'aids': temp_aids}
                        temp_aids.append(str(item['aid']))
                        logger.info(f"[分区 {search_options.video_zone_type}] 发现视频: {item['aid']} (关键词 {keyword} 第{keyword_pages[keyword]}页)")
                    
                    # 返回本次抓取结果，并标记该关键词未结束
                    return {'end': end, 'keyword': keyword, 'aids': temp_aids}

            # 创建并执行一批并发任务
            tasks = [sem_fetch(keyword) for keyword in current_batch]
            results = await asyncio.gather(*tasks)

            # 处理并发任务的结果
            for result in results:
                keyword = result['keyword']
                aids.extend(result['aids'])
                # 如果某个关键词的搜索结束了，就从活动列表中移除
                if result['end']:
                    if keyword in active_keywords:
                        active_keywords.remove(keyword)
                else:
                    # 否则，该关键词的页码加一，准备搜索下一页
                    keyword_pages[keyword] += 1

            # 轮换关键词顺序
            if current_batch:
                remaining_keywords = [k for k in active_keywords if k not in current_batch]
                active_keywords = remaining_keywords + [k for k in current_batch if k in active_keywords]

            await asyncio.sleep(self.config.SLEEP_TIME * 2)

        return list(set(aids))
    
    async def get_batch_details_by_aid(self, aids: List[int], need_extra: bool = False) -> Dict[int, Dict[str, Any]]:
        """
        使用B站medialist接口批量获取视频的详细信息。
        
        Args:
            aids: 视频aid整数列表。
            need_extra: 是否需要进行额外检查（如过滤短视频）。
        
        Returns:
            一个从aid映射到视频信息字典的字典。
        """
        BATCH_SIZE = 50
        all_stats = {}
        session = await self.get_session()

        # 将所有aid按BATCH_SIZE分批处理
        for i in range(0, len(aids), BATCH_SIZE):
            batch_aids = aids[i:i + BATCH_SIZE]
            resources_str = ",".join([f"{aid}:2" for aid in batch_aids])
            url = f"https://api.bilibili.com/medialist/gateway/base/resource/infos?resources={resources_str}"
            
            logger.info(f"正在通过 medialist 接口处理批次 {i//BATCH_SIZE + 1}，包含 {len(batch_aids)} 个视频...")

            try:
                async with session.get(url, headers={'User-Agent': random.choice(self.config.HEADERS)}, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == 0 and data.get('data'):
                            for item in data['data']:
                                if need_extra and item.get('duration', 0) <= self.config.MIN_VIDEO_DURATION:
                                    logger.debug(f"跳过短视频: {item['id']}")
                                    continue
                                all_stats[item['id']] = item
                        else:
                            logger.warning(f"API 返回错误或无数据，批次：{batch_aids}, 响应: {data.get('message', 'N/A')}")
                    else:
                        logger.error(f"请求批次失败，HTTP状态码: {response.status}, 批次: {batch_aids}")
                # 在处理完一个批次后稍作等待
                await asyncio.sleep(self.config.SLEEP_TIME)

            except Exception as e:
                logger.error(f"处理批次时发生异常: {e}, 批次: {batch_aids}")

        return all_stats

    async def get_all_aids(self) -> List[str]:
        """
        综合搜索和分区两种方式获取目标视频的aid列表。
        
        Returns:
            去重后的aid字符串列表。
        """
        # 首先通过关键词搜索获取aid
        aids: Set[str] = set()

        for search_option in self.search_options:
            if self.mode == "new":
                search_option.time_start = self.start_time.strftime('%Y-%m-%d')
                search_option.time_end = self.today.strftime('%Y-%m-%d')
            # 对每个分区进行搜索
            aids.update(await self.get_video_list_by_search_for_zone(search_option))
            await asyncio.sleep(self.config.SLEEP_TIME)

        # 如果是新曲模式，额外通过分区最新列表获取aid，作为补充
        if self.mode == "new":
            aids.update(await self.get_video_list_by_zone())
        return list(set(aids))  
    
    async def get_video_details(self, aids: List[str]) -> List[VideoInfo]:
        """
        根据aid列表，批量获取并构建视频详细信息对象列表。
        
        Args:
            aids: 需要获取详情的视频aid字符串列表。
                
        Returns:
            成功获取的VideoInfo对象列表。
        """
        int_aids = [int(aid) for aid in aids if aid and aid.isdigit()]
        if not int_aids:
            return []
            
        need_extra = self.mode in ["new", "special"]
        stats = await self.get_batch_details_by_aid(int_aids, need_extra=need_extra)
        
        videos: List[VideoInfo] = []
        for aid, info in stats.items():
            try:
                title = clean_tags(info.get('title', ''))
                if title == "已失效视频":
                    raise VideoInvalidException(f"视频 {aid} 已失效。")
                
                # 'old' 模式下，从已有的表格中读取部分不会改变或需要保留的元数据
                existing_data = {}
                if self.mode == "old":
                    song_data = self.songs[self.songs['aid'] == str(aid)].iloc[0]
                    existing_data = {
                        'bvid': song_data['bvid'],   
                        'aid': str(aid),
                        'name': song_data['name'],
                        # 版权信息优先使用API获取的，除非已有数据不是1或2
                        'copyright': song_data['copyright'] if song_data['copyright'] not in [1, 2] else info.get('copyright', 1),
                        'author': song_data['author'],
                        'synthesizer': song_data['synthesizer'],
                        'vocal': song_data['vocal'],
                        'type': song_data['type']
                    }
                # 使用解包语法创建VideoInfo对象
                # 如果是'old'模式，使用existing_data填充；否则，直接从API数据构建
                video_info = VideoInfo(
                    **existing_data if self.mode == "old" else {
                        'bvid': info.get('bvid', ''),
                        'aid': str(aid),
                        'name': clean_tags(info.get('title', '')),
                        'copyright': info.get('copyright', 1),
                        'author': info.get('upper', {}).get('name', ''),
                        'synthesizer': "",
                        'vocal': "",
                        'type': "",
                    },
                    # 以下是所有模式都需要从API更新的数据
                    pubdate=datetime.fromtimestamp(info.get('pubtime', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                    title=title,
                    uploader=info.get('upper', {}).get('name', ''),
                    duration=convert_duration(info.get('duration', 0)),
                    page=info.get('page', 1),
                    view=info.get('cnt_info', {}).get('play', 0),
                    favorite=info.get('cnt_info', {}).get('collect', 0),
                    coin=info.get('cnt_info', {}).get('coin', 0),
                    like=info.get('cnt_info', {}).get('thumb_up', 0),
                    image_url=info.get('cover', '')
                )
                videos.append(video_info)
                
            except Exception as e:
                logger.error(f"处理视频 {aid} 信息时出错: {e}")
                
        return videos

    def update_recorded_songs(self, videos: List[VideoInfo], census_mode: bool):
        """
        根据新获取的视频信息，更新已收录曲目的数据，并处理其状态。
           
        Args:
            videos: 需要更新的VideoInfo对象列表。
            census_mode: 是否为普查模式。
        """
        # 将新获取的视频信息转换为DataFrame
        update_df = pd.DataFrame([{
            'bvid': video.bvid,
            'aid': video.aid,
            'title': video.title,
            'view': video.view,
            'uploader': video.uploader,
            'copyright': video.copyright,
            'image_url': video.image_url,
        } for video in videos])
        # 备份更新前的播放量数据，用于后续计算增量
        old_views = self.songs.set_index('bvid')['view']
        # 计算并标记哪些视频已失效
        self.songs['is_failed'] = calculate_failed_mask(self.songs, update_df, census_mode, self.config.STREAK_THRESHOLD)
        # 将bvid设为索引，以便使用update方法
        self.songs = self.songs.set_index('bvid')
        update_df = update_df.set_index('bvid')
        # 使用update方法，用新数据批量更新旧数据
        self.songs.update(update_df)
        # 处理所有视频的连续未达标计数
        self.process_streaks(old_views, update_df.index, census_mode)
        # 恢复索引，并按失效状态和播放量进行排序
        self.songs = self.songs.reset_index().sort_values(['is_failed', 'view'], ascending=[False, False]).drop('is_failed', axis=1)
        # 保存更新后的数据到Excel文件
        save_to_excel(self.songs, "收录曲目.xlsx", usecols=json.load(Path('config/usecols.json').open(encoding='utf-8'))["columns"]["record"])

    async def process_new_songs(self) -> List[Dict[str, Any]]:
        """
        执行抓取新曲数据的完整流程。
        
        Returns:
            新发布视频的详细信息字典列表。
        """
        logger.info("开始获取新曲数据")
        # 获取所有符合条件的aid
        aids = await self.get_all_aids()
        logger.info(f"一共有 {len(aids)} 个 aid")
        # 根据aid获取视频详细信息
        videos = await self.get_video_details(aids)
        
        videos = [video for video in videos if datetime.strptime(video.pubdate, '%Y-%m-%d %H:%M:%S') > self.start_time]
        # 将VideoInfo对象列表转换为字典列表
        return [asdict(video) for video in videos]
    
    async def process_old_songs(self) -> List[Dict[str, Any]]:
        """
        执行更新已收录歌曲数据的完整流程。
        
        Returns:
            已更新视频的详细信息字典列表。
        """
        logger.info("开始获取旧曲数据")
        # 判断当天是否为普查日
        census_mode = self.is_census_day()

        if census_mode:
            # 普查模式下，处理所有已收录的歌曲
            songs_to_process_df = self.songs
            logger.info(f"普查模式：准备处理全部 {len(songs_to_process_df)} 个视频")
        else:
            # 常规模式下，只处理连续未达标次数低于阈值的歌曲
            mask = self.songs['streak'] < self.config.STREAK_THRESHOLD
            songs_to_process_df = self.songs.loc[mask]
            logger.info(f"常规模式：准备处理 {len(songs_to_process_df)} 个视频")

        if songs_to_process_df.empty:
            logger.info("没有需要处理的旧曲，任务结束。")
            return []
        # 获取待处理视频的详细信息
        videos = await self.get_video_details(songs_to_process_df['aid'].tolist())
        # 更新数据库并处理streak状态
        self.update_recorded_songs(videos, census_mode)
        # 返回更新后的视频信息
        return [asdict(v) for v in videos]
    
    async def save_to_excel(self, videos: List[Dict[str, Any]], usecols: Optional[List[str]] = None) -> None:
        """保存到Excel文件。"""
        df = pd.DataFrame(videos)
        df = df.sort_values(by='view', ascending=False)
        save_to_excel(df, self.filename, usecols=usecols)
