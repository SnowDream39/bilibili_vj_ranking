# config/dataclass.py
from dataclasses import dataclass, field
from typing import List, Optional
from pathlib import Path
import json
from bilibili_api import search

@dataclass
class VideoInfo:
    """存储从B站API和已有数据库获取的视频详细信息。"""
    title: str
    bvid: str
    aid: str
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
    intro: str = ""
    streak: int = 0

@dataclass    
class VideoInvalidException(Exception):
    """自定义异常，用于表示视频已失效或无法访问。"""
    message: str
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

@dataclass
class SearchOptions:
    """B站搜索参数配置类"""
    search_type: search.SearchObjectType = search.SearchObjectType.VIDEO
    order_type: search.OrderVideo = search.OrderVideo.PUBDATE
    video_zone_type: Optional[int] = 0
    order_sort: Optional[int] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None
    page_size: Optional[int] = 50
    newlist_rids: List[int] = field(default_factory=list)

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
    MAX_RETRIES: int = 5
    SEMAPHORE_LIMIT: int = 5
    MIN_VIDEO_DURATION: int = 20
    SLEEP_TIME: float = 0.2
    OUTPUT_DIR: Path = Path("新曲数据")
    NAME: Optional[str] = None
    STREAK_THRESHOLD: int = 7
    MIN_TOTAL_VIEW: int = 10000
    BASE_THRESHOLD: int = 100
    LOCAL_METADATA_FIELDS: List[str] = field(default_factory=lambda: [
        'bvid', 'name', 'author', 'copyright', 'synthesizer', 'vocal', 'type'
    ])
    UPDATE_COLS: List[str] = field(default_factory=lambda: [
        'bvid', 'aid', 'title', 'view', 'uploader', 'copyright', 'image_url'
    ])

    @staticmethod
    def load_keywords(file_path: str = "keywords.json") -> List[str]:
        """从JSON文件加载搜索关键词"""
        with open(Path(file_path), "r", encoding="utf-8") as f:
            return json.load(f)
