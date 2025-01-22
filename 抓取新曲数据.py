import asyncio
import aiohttp
import pandas as pd
from bilibili_api import search, video, Credential
from datetime import datetime, timedelta
import re
import random
from openpyxl.utils import get_column_letter
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from pathlib import Path


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

class Config:
    """配置类"""
    KEYWORDS = [
    "MEIKO", "KAITO", 
    "初音未来", "ミク", "miku", "初音",
    "镜音铃", "鏡音リン", "Rin", "镜音连", "鏡音レン", "Len", 
    "Prima", "巡音流歌", "ルカ", "Luka",
    "神威乐步", "Gackpoid", "神威がくぽ", "GUMI", "Megpoid",
    "SONiKA", "冰山清辉", "氷山キヨテル", "Hiyama Kiyoteru",
    "歌爱雪", "歌爱YUKI", "歌愛ユキ", "miki", "Lily",
    "结月缘", "結月ゆかり", "Yuzuki Yukari", "IA",
    "苍姬拉碧斯", "蒼姫ラピス", "Aoki Lapis", "洛天依",
    "Galaco", "ギャラ子", "MAYU", "AVANNA", "KYO", "WIL", "YUU",
    "言和", "V flower", "Ci flower", "flower",
    "东北俊子", "东北ずん子", "ずん子", "Rana", "Chika",
    "心华", "心華", "乐正绫", "Sachiko", "幸子", "Ruby",
    "DAINA", "DEX", "Fukase", "星尘", "星尘stardust",
    "音街鳗", "音街ウナ", "UNI", "乐正龙牙", "LUMi",
    "绁星灯", "紲星あかり", "徵羽摩柯", "墨清弦",
    "樱乃空", "桜乃そら", "鸣花姬", "鳴花ヒメ",
    "鸣花尊", "鳴花ミコト", "Po-uta", "战音Lorra", "Ken",
    "呗音Uta", "唄音ウタ", "Defo子", "デフォ子", "默认子",
    "重音Teto", "重音テト", "teto", "桃音Momo", "桃音モモ",
    "欲音Ruko", "欲音ルコ", "波音律", "波音リツ",
    "健音帝", "健音テイ", "雪歌Yufu", "雪歌ユフ",
    "东北伊达子", "東北イタコ", "イタコ",
    "东北切蒲英", "東北きりたん", "きりたん",
    "小感冒", "Kazehiki", "カゼヒキ", "剧药", "Gekiyaku", "ゲキヤク",
    "旭音Ema", "足立零", "足立レイ", "足立Rei",
    "可不", "kafu", "星界", "SEKAI",
    "知声", "Chis-A", "里命", "RIME", "裏命",
    "POPY", "ROSE", "狐子", "COKO", "羽累", "HARU",
    "月读爱", "弦卷真纪", "弦巻マキ", "Tsurumaki Maki",
    "琴叶茜", "琴葉茜", "琴叶葵", "琴葉葵",
    "京町精华", "京町セイカ", "追傩酱", "ついなちゃん",
    "永夜Minus", "Minus", "小春六花", "夏色花梨",
    "花隈千冬", "Mai", "奕夕", "绮萱", "俊达萌", "ずんだもん",
    "whiteCUL", "NurseRobot TypeT", "ナースロボ＿タイプＴ", "诗岸", 
    "四国玫碳", "四国めたん", "小夜",
    "东方栀子", "宫舞茉歌", "宮舞モカ", "Eri", "夏语遥",
    "SOLARIA", "POYOROID", "韵泉", "春日部",
    "葛駄夜音", "艾可", "赤羽", 
    "Koronba4号", "ころんば4号", "默辰", "沨漪",
    "VOCALOID", "synthesizer v", "SynthV", "SV", "CeVIO", "UTAU", "VOICEROID", 
    "VOICEPEAK", "NEUTRINO","术力口","ボカロ","ボーカロイド","Voisona","NT",
    "夢ノ結唱", "日文", "chinozo", 
    "rotbala", "regnore", "青杉折扇", "阿赫official", "SYNZI",
    "羊小星", "精神安定剤", "KitanoNani", "妄想Delusions", "苏维埃冰棺中的伊利亚",
    "委蛇原_radio", "kttts", "珠紫MuRaSaKi", "音街ウナ", "SenaRinka_Alice",
    "雨喙Beak_In_Rain", "星のカケラ", "Soraだよ", "BoringCoumselor",
    "ゆりがさきなな", "SILVIA____", "trance羯", "911", "等待呢歌","空想水晶诅咒绯红锁链",
    "BILI君的音樂工房", "折射", "唯爱阿萌", "牛牛蝎羯", "白羽沉","VOCALOID音乐社",
    "【存在抹消】", "かた方"
    ]
    HEADERS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.67 Safari/537.36',
    ]

    MAX_RETRIES = 3
    SEMAPHORE_LIMIT = 5
    MIN_VIDEO_DURATION = 20
    SLEEP_TIME = 0.8
    OUTPUT_DIR = Path("新曲数据")

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
    def __init__(self, days: int = 2):
        self.days = days
        self.today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.start_time = self.today - timedelta(days=days)
        Config.OUTPUT_DIR.mkdir(exist_ok=True)

    @staticmethod
    def clean_html_tags(text: str) -> str:
        return re.sub(r'<.*?>', '', text)

    @staticmethod
    def convert_duration(duration: int) -> str:
        duration -= 1
        minutes, seconds = divmod(duration, 60)
        return f'{minutes}分{seconds}秒' if minutes > 0 else f'{seconds}秒'

    async def fetch_data(self, url: str) -> Optional[Dict]:
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': random.choice(Config.HEADERS)}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
        return None

    async def search_videos(self, keyword: str) -> List[str]:
        bvids = []
        page = 1

        while True:
            print(f"Searching for keyword: {keyword}, page: {page}")
            result = await search.search_by_type(
                keyword,
                search_type=search.SearchObjectType.VIDEO,
                order_type=search.OrderVideo.PUBDATE,
                video_zone_type=3,
                page=page
            )

            videos = result.get('result', [])
            if not videos:
                break

            for item in videos:
                pubdate = datetime.fromtimestamp(item['pubdate'])
                if pubdate >= self.start_time:
                    bvids.append(item['bvid'])
                    print(f"发现视频： {item['bvid']}")
                else:
                    return bvids
            page += 1

        return bvids

    async def fetch_video_list(self, rid: int, ps: int = 50, pn: int = 1) -> Optional[Dict]:
        url = f"https://api.bilibili.com/x/web-interface/newlist?rid={rid}&ps={ps}&pn={pn}"
        return await RetryHandler.retry_async(self.fetch_data, url)

    async def get_all_bvids(self) -> List[str]:
        all_bvids = set()

        # 从关键词搜索获取视频
        for keyword in Config.KEYWORDS:
            bvids = await self.search_videos(keyword)
            all_bvids.update(bvids)

        # 从分区最新获取视频
        rid = 30
        page_num = 1
        while True:
            video_list = await self.fetch_video_list(rid, pn=page_num)
            if not video_list or 'data' not in video_list or 'archives' not in video_list['data']:
                break

            recent_videos = [
                video for video in video_list['data']['archives']
                if datetime.fromtimestamp(video['pubdate']) > self.start_time
            ]
            if not recent_videos:
                break

            all_bvids.update(video['bvid'] for video in recent_videos)
            page_num += 1
            await asyncio.sleep(0.2)

        return list(all_bvids)

    async def fetch_video_details(self, bvid: str) -> Optional[VideoInfo]:
        try:
            v = video.Video(bvid, credential=Credential())
            info = await v.get_info()
            
            if info['duration'] <= Config.MIN_VIDEO_DURATION:
                print(f"跳过短视频： {bvid}")
                return None
            print(f"获取视频信息： {bvid}")
            return VideoInfo(
                title=self.clean_html_tags(info['title']),
                bvid=bvid,
                name=self.clean_html_tags(info['title']),
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
                image_url=info['pic']
            )
        except Exception as e:
            print(f"Error fetching details for {bvid}: {str(e)}")
            return None

    async def get_video_details(self, bvids: List[str]) -> List[VideoInfo]:
        sem = asyncio.Semaphore(Config.SEMAPHORE_LIMIT)
        
        async def sem_fetch(bvid: str) -> Optional[VideoInfo]:
            async with sem:
                result = await RetryHandler.retry_async(self.fetch_video_details, bvid)
                await asyncio.sleep(Config.SLEEP_TIME)
                return result

        tasks = [sem_fetch(bvid) for bvid in bvids]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def process_videos(self) -> List[Dict[str, Any]]:
        print("Starting to get all bvids")
        bvids = await self.get_all_bvids()
        print(f"Total bvids found: {len(bvids)}")
        
        videos = await self.get_video_details(bvids)
        return [asdict(video) for video in videos]

    async def save_to_excel(self, videos: List[Dict[str, Any]]) -> None:
        df = pd.DataFrame(videos)
        df = df.sort_values(by='view', ascending=False)

        columns = ['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 
                  'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 
                  'view', 'favorite', 'coin', 'like', 'image_url']
        
        df = df[columns]
        filename = Config.OUTPUT_DIR / f"新曲{self.today.strftime('%Y%m%d')}.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']
            
            pubdate_col = get_column_letter(df.columns.get_loc('pubdate') + 1)
            for cell in worksheet[pubdate_col]:
                cell.number_format = '@'
                cell.alignment = cell.alignment.copy(horizontal='left')

        print(f"{filename} 已保存")

async def main():
    scraper = BilibiliScraper()
    videos = await scraper.process_videos()
    await scraper.save_to_excel(videos)

if __name__ == "__main__":
    asyncio.run(main())
