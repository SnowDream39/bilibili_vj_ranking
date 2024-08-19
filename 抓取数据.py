import asyncio
import subprocess
import pandas as pd
from datetime import datetime
from bilibili_api import video

class SongDataFetcher:
    def __init__(self, input_file, output_dir):
        self.input_file = input_file
        self.output_dir = output_dir
        self.songs = pd.read_excel(input_file)
        self.info_list = []
        self.error_list = []
        self.data_list = []
        self.semaphore = asyncio.Semaphore(5)  # 限制并发任务数为10

    def run_script_in_new_window(self, script):
        subprocess.Popen(['start', 'cmd', '/k', f'python {script}'], shell=True)

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
            try:
                info = await v.get_info()
                await asyncio.sleep(0.1)  # 添加延迟，避免请求过于频繁
            except Exception as e:
                print(f"Error fetching data for: {song['Title']} ({song['BVID']}), error: {e}")
                self.error_list.append(i)
                return

            stat_data = info.get('stat')
            owner_data = info.get('owner')
            duration = self.convert_duration(info.get('duration'))

            if stat_data and owner_data:
                view = stat_data.get('view')
                favorite = stat_data.get('favorite')
                coin = stat_data.get('coin')
                like = stat_data.get('like')
                
                self.info_list.append([
                    song['Video Title'], song['BVID'], song['Title'], 
                    song['Author'], song['Uploader'], song['Copyright'], 
                    song['Synthesizer'], song['Vocal'], song['Type'], song['Pubdate'], 
                    duration, view, favorite, coin, like
                ])
                self.data_list.append([
                    song['Title'], song['BVID'], song['Video Title'], 
                    view, song['Pubdate'], song['Author'], song['Uploader'], 
                    song['Copyright'], song['Synthesizer'], song['Vocal'], song['Type']
                ])
            else:
                print(f"Missing data for: {song['Title']} ({song['BVID']})")
                self.error_list.append(i)
            await asyncio.sleep(0.8)  # 每次执行任务后等待0.8秒

    async def fetch_all_stats(self):
        await self.run_tasks_with_retries(self.songs.index)

    async def run_tasks_with_retries(self, indices, max_retries=5):
        for attempt in range(max_retries):
            tasks = [self.fetch_song_stat(i) for i in indices]
            await asyncio.gather(*tasks)

            if not self.error_list:
                break
            print(f"Retrying for error list, attempt {attempt + 1}...")
            indices, self.error_list = self.error_list, []

    def save_data(self):
        if self.info_list:
            stock_list = pd.DataFrame(self.info_list, columns=[
                'video_title', 'bvid', 'title', 'author', 'uploader', 
                'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 
                'duration', 'view', 'favorite', 'coin', 'like'
            ])
            filename = f"{self.output_dir}/{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
            stock_list.to_excel(filename, index=False)
            print(f"处理完成，数据已保存到 {filename}")

        if self.data_list:
            # 创建 DataFrame
            new_stock_list = pd.DataFrame(self.data_list, columns=[
                'Title', 'BVID', 'Video Title', 'View', 'Pubdate', 
                'Author', 'Uploader', 'Copyright', 'Synthesizer', 'Vocal', 'Type'
            ])
            new_stock_list = new_stock_list.sort_values(by='View', ascending=False)

            # 读取原始数据
            original_songs = pd.read_excel(self.input_file)

            # 合并数据
            merged_stock_list = pd.concat([original_songs, new_stock_list]).drop_duplicates(subset=['BVID'], keep='last')

            # 保存数据
            merged_stock_list.to_excel(self.input_file, index=False)
            print("收录曲目已更新并按观看数排序")
        else:
            print("没有可用的视频信息，未保存数据")

    async def run(self):
        await self.fetch_all_stats()
        self.save_data()

if __name__ == "__main__":
    fetcher = SongDataFetcher(input_file='收录曲目.xlsx', output_dir='数据')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetcher.run())
