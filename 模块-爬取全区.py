import requests
import pandas as pd
import datetime
import time
import re
from utils.logger import setup_logger
from utils.io_utils import save_to_excel
logger = setup_logger(__name__)
cate_id = 30
# 移除非法字符
def remove_illegal_characters(text):
    return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F\u200B-\u200F\u2028-\u202F\u205F-\u206F\uFEFF\uFFFE-\uFFFF]', '', text)

# 读取已收录的 bvid
def load_existing_bvids(file_path):
    try:
        existing_df = pd.read_excel(file_path, usecols=['bvid'])
        return set(existing_df['bvid'].dropna().astype(str))
    except FileNotFoundError:
        logger.warning(f"{file_path} 不存在，未加载任何已收录的 bvid。")
        return set()

def get_videos(time_from, time_to, existing_bvids, max_retries=5):
    url = 'https://api.bilibili.com/x/web-interface/newlist_rank'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Referer': 'https://www.bilibili.com',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    params = {
        'main_ver': 'v3',
        'search_type': 'video',
        'view_type': 'hot_rank',
        'copy_right': '-1',
        'order': 'click',
        'cate_id': cate_id,
        'page': '1',
        'pagesize': '50',
        'time_from': time_from,
        'time_to': time_to
    }

    all_videos = []
    retries = 0

    while retries < max_retries:
        logger.info(f"正在请求从 {time_from} 到 {time_to} 的数据，第 {params['page']} 页...")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            logger.error(f"获取数据失败，状态码: {response.status_code}")  
            retries += 1
            time.sleep(2 ** retries)
            continue

        data = response.json()

        if 'data' in data and 'result' in data['data']:
            videos = data['data']['result']
            if not videos:
                logger.warning("当前页面未找到视频，正在重试...")  
                retries += 1
                time.sleep(2 ** retries)
                continue

            retries = 0
            logger.info(f"在当前页面找到 {len(videos)} 个视频。")  
            for video in videos:
                try:
                    view_count = int(video['play'])
                    duration = int(video['duration'])

                    # 排除已存在的 bvid 和时长小于 20 秒的视频
                    if video['bvid'] in existing_bvids:
                        logger.info(f"排除收录视频： {video['bvid']}")
                        continue
                    if duration <= 20:
                        logger.info(f"排除短视频: {video['title']}")
                        continue
                    if view_count < 10000 and view_count:
                        logger.info(f"发现视频: {video['title']}, View: {view_count}")
                        return all_videos

                    # 移除标题中的非法字符
                    title = remove_illegal_characters(video['title'])
                    video_data = {
                        'title': title,
                        'bvid': video['bvid'],
                        'aid': video['id'],
                        'view': view_count,
                        'pubdate': video['pubdate'],
                        'author': video['author'],
                        'image_url': video['pic']
                    }
                    all_videos.append(video_data)
                    logger.info(f"Title: {video_data['title']}, bvid: {video_data['bvid']}, View: {video_data['view']}")
                except Exception as e:
                    logger.error(f"处理视频数据时出错: {e}")  
            params['page'] = str(int(params['page']) + 1)
            time.sleep(0) 
        else:
            logger.info(f"在 {time_from} 到 {time_to} 之间未找到更多数据。")  
            break

    return all_videos

def get_all_videos(start_date, end_date, output_prefix, existing_bvids):
    all_videos = []
    current_date = start_date

    while current_date >= end_date:
        next_date = max(current_date - datetime.timedelta(days=90), end_date)
        time_from = next_date.strftime('%Y%m%d')
        time_to = current_date.strftime('%Y%m%d')
        output_file = f'{cate_id}-{output_prefix}_{time_from}_to_{time_to}.xlsx'

        videos = get_videos(time_from, time_to, existing_bvids)
        if videos:
            all_videos.extend(videos)
            df = pd.DataFrame(videos)
            save_to_excel(df, output_file)

        current_date = next_date - datetime.timedelta(days=1)
        time.sleep(1) 

    return all_videos

def main():
    today = datetime.datetime.now().strftime('%Y%m%d')
    start_date = datetime.datetime.strptime(today, '%Y%m%d')
    end_date = datetime.datetime.strptime((start_date - datetime.timedelta(days=15)).strftime('%Y%m%d'), '%Y%m%d')
    output_prefix = 'bilibili_videos'

    existing_bvids = load_existing_bvids("收录曲目.xlsx")

    get_all_videos(start_date, end_date, output_prefix, existing_bvids)

if __name__ == '__main__':
    main()