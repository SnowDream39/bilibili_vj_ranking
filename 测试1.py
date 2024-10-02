import asyncio
import pandas as pd
from datetime import datetime
from bilibili_api import video
import aiohttp
import random

class SongDataFetcher:
    def __init__(self, input_file, output_dir, max_concurrent_tasks=10):
        self.input_file = input_file
        self.output_dir = output_dir
        self.songs = pd.read_excel(input_file)
        self.info_list = []
        self.error_list = []
        self.data_list = []
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)  # 控制最大并发数
        self.session = None  # aiohttp 会话，用于批量请求
        self.headers = self.get_headers()
          
    def get_headers(self):
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'
        ]
        return {
            'User-Agent': random.choice(user_agents),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def convert_duration(self, duration):
        duration -= 1
        minutes, seconds = divmod(duration, 60)
        if minutes > 0:
            return f'{minutes}分{seconds}秒'
        else:
            return f'{seconds}秒'

    async def fetch_song_stat(self, i):
        async with self.semaphore:  # 使用信号量控制并发数量
            song = self.songs.loc[i]
            print(f"Fetching data for: {song['Title']} ({song['BVID']})")
            v = video.Video(bvid=song['BVID'])
            retry_count = 0
            while retry_count < 5:  # 重试机制
                try:
                    info = await v.get_info()
                    await asyncio.sleep(random.uniform(0.3, 0.8))  # 动态延迟，避免封禁
                    break  # 成功后退出循环
                except Exception as e:
                    retry_count += 1
                    print(f"Error fetching data for {song['Title']} ({song['BVID']}), retry {retry_count}, error: {e}")
                    await asyncio.sleep(0.5 + retry_count)  # 指数退避延迟

            if retry_count >= 5:
                self.error_list.append(i)
                return

            stat_data = info.get('stat')
            owner_data = info.get('owner')

            video_title = info.get('title')
            duration = self.convert_duration(info.get('duration'))
            image_url = info.get('pic')  

            if stat_data and owner_data:
                view = stat_data.get('view')
                favorite = stat_data.get('favorite')
                coin = stat_data.get('coin')
                like = stat_data.get('like')

                self.info_list.append([
                    video_title, song['BVID'], song['Title'], 
                    song['Author'], song['Uploader'], song['Copyright'], 
                    song['Synthesizer'], song['Vocal'], song['Type'], song['Pubdate'], 
                    duration, view, favorite, coin, like, image_url
                ])
                self.data_list.append([
                    song['Title'], song['BVID'], video_title, 
                    view, song['Pubdate'], song['Author'], song['Uploader'], 
                    song['Copyright'], song['Synthesizer'], song['Vocal'], song['Type'], image_url
                ])
            else:
                print(f"Missing data for: {song['Title']} ({song['BVID']})")
                self.error_list.append(i)

    async def fetch_all_stats(self):
        tasks = [self.fetch_song_stat(i) for i in self.songs.index]
        await asyncio.gather(*tasks)

    def save_data(self):
        if self.info_list:
            stock_list = pd.DataFrame(self.info_list, columns=[
                'video_title', 'bvid', 'title', 'author', 'uploader', 
                'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 
                'duration', 'view', 'favorite', 'coin', 'like', 'image_url'
            ])
            filename = f"{self.output_dir}/{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
            stock_list.to_excel(filename, index=False)
            print(f"处理完成，数据已保存到 {filename}")

        if self.data_list:
            new_stock_list = pd.DataFrame(self.data_list, columns=[
                'Title', 'BVID', 'Video Title', 'View', 'Pubdate', 
                'Author', 'Uploader', 'Copyright', 'Synthesizer', 'Vocal', 'Type', 'image_url'
            ])
            new_stock_list = new_stock_list.sort_values(by='View', ascending=False)
            original_songs = pd.read_excel(self.input_file)
            merged_stock_list = pd.concat([original_songs, new_stock_list]).drop_duplicates(subset=['BVID'], keep='last')
            merged_stock_list.to_excel(self.input_file, index=False)
            print("收录曲目已更新并按观看数排序")
        else:
            print("没有可用的视频信息，未保存数据")

    async def run(self):
        async with aiohttp.ClientSession() as session:  # 使用aiohttp批量请求
            self.session = session
            await self.fetch_all_stats()
            self.save_data()

if __name__ == "__main__":
    fetcher = SongDataFetcher(input_file='收录曲目.xlsx', output_dir='数据', max_concurrent_tasks=10)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetcher.run())
