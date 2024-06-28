#coding:GB2312
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta

async def get_video_list(rid, ps=20, pn=1):
    url = f"https://api.bilibili.com/x/web-interface/newlist?rid={rid}&ps={ps}&pn={pn}"
    headers = {
        "User-Agent": "9bishi"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            else:
                print("Error fetching video list")
                return None

async def get_video_stat(video):
    """从单个视频信息中提取所有所需详细数据"""
    return {
        'bvid': video['bvid'],
        'title': video['title'],
        'pubdate': datetime.fromtimestamp(video['pubdate']).strftime('%Y-%m-%d %H:%M:%S'),
        'view': video['stat']['view'],
        'danmaku': video['stat']['danmaku'],
        'reply': video['stat']['reply'],
        'favorite': video['stat']['favorite'],
        'coin': video['stat']['coin'],
        'share': video['stat']['share'],
        'like': video['stat']['like']
    }

async def fetch_and_process_page(rid, page_num, days, semaphore):
    async with semaphore:
        video_list_page = await get_video_list(rid, pn=page_num)
        if not video_list_page or 'data' not in video_list_page or 'archives' not in video_list_page['data']:
            print("No more videos or error fetching page.")
            return [], False

        recent_videos_on_page = [video for video in video_list_page['data']['archives'] if datetime.fromtimestamp(video['pubdate']) > datetime.now() - timedelta(days=days)]
        if not recent_videos_on_page:  # If no recent videos on this page, we've likely hit the boundary
            return [], True

        full_data_list = [await get_video_stat(video) for video in recent_videos_on_page]
        for data in full_data_list:
            print(f"Bv号: {data['bvid']}, 播放量: {data['view']}")
        print(f"Processed page {page_num}, found {len(recent_videos_on_page)} recent videos.")
        return full_data_list, False

async def main(rid=30, days=3):
    semaphore = asyncio.Semaphore(5)  # 控制并发数
    all_videos_data = []
    page_num = 1

    while True:
        results, should_break = await fetch_and_process_page(rid, page_num, days, semaphore)
        all_videos_data.extend(results)
        if should_break:
            break
        page_num += 1
        await asyncio.sleep(0.5) 

    # 所有数据处理完成后，导出到Excel文件
    df = pd.DataFrame(all_videos_data)
    df = df.sort_values(by='view', ascending=False)
    filename = "新曲数据/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".xlsx"
    df.to_excel(filename, index=False)
    print("处理完成，数据已保存到", filename)

if __name__ == "__main__":
    asyncio.run(main())
