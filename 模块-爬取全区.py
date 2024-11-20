import requests
import pandas as pd
import datetime
import time
import re
from openpyxl import load_workbook

# 移除非法字符
def remove_illegal_characters(text):
    return re.sub(r'[\x00-\x1F\x7F]', '', text)

# 读取已收录的 bvid
def load_existing_bvids(file_path):
    try:
        existing_df = pd.read_excel(file_path, usecols=['BVID'])
        return set(existing_df['BVID'].dropna().astype(str))
    except FileNotFoundError:
        print(f"{file_path} 不存在，未加载任何已收录的 bvid。")
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
        'new_web_tag': '1',
        'order': 'click',
        'cate_id': '30',
        'page': '1',
        'pagesize': '30',
        'time_from': time_from,
        'time_to': time_to
    }
    
    all_videos = []
    retries = 0

    while retries < max_retries:
        print(f"Requesting data from {time_from} to {time_to} with page {params['page']}...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code}")
            retries += 1
            time.sleep(2 ** retries)  
            continue
        
        data = response.json()
        
        if 'data' in data and 'result' in data['data']:
            videos = data['data']['result']
            if not videos:
                print(f"No videos found in this page. Retrying...")
                retries += 1
                time.sleep(2 ** retries)  
                continue

            retries = 0  
            print(f"Found {len(videos)} videos in this page.")
            for video in videos:
                try:
                    view_count = int(video['play'])
                    duration = int(video['duration']) 

                    # 排除已存在的 bvid 和时长小于 20 秒的视频
                    if video['bvid'] in existing_bvids:
                        print(f"Skipping duplicate bvid: {video['bvid']}")
                        continue
                    if duration <= 20:
                        print(f"Skipping short duration video (less than 20 seconds): {video['title']}")
                        continue
                    if view_count < 10000:
                        print(f"Found a video with less than 100,000 views: {video['title']}, View: {view_count}")
                        return all_videos  

                    # 移除标题中的非法字符
                    title = remove_illegal_characters(video['title'])
                    video_data = {
                        'title': title,
                        'bvid': video['bvid'],
                        'author': video['author'],
                        'view': view_count,
                        'pubdate': video['pubdate']
                    }
                    all_videos.append(video_data)
                    print(f"Title: {video_data['title']}, BVID: {video_data['bvid']}, View: {video_data['view']}")
                except Exception as e:
                    print(f"Error processing video data: {e}")
            params['page'] = str(int(params['page']) + 1)
        else:
            print(f"No more data found from {time_from} to {time_to}.")
            break
    
    return all_videos

def get_all_videos(start_date, end_date, output_file, existing_bvids):
    all_videos = []
    current_date = start_date
    
    while current_date >= end_date:
        next_date = max(current_date - datetime.timedelta(days=90), end_date)
        time_from = next_date.strftime('%Y%m%d')
        time_to = current_date.strftime('%Y%m%d')
        
        videos = get_videos(time_from, time_to, existing_bvids)
        if videos:
            all_videos.extend(videos)
            df = pd.DataFrame(videos)
            append_to_excel(df, output_file)

        current_date = next_date - datetime.timedelta(days=1) 
    
    return all_videos

def append_to_excel(df, file_path):
    try:
        with pd.ExcelWriter(file_path, mode='a', if_sheet_exists='overlay', engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=writer.sheets is None, startrow=writer.sheets['Sheet1'].max_row if 'Sheet1' in writer.sheets else 0)
            print(f'Data appended to {file_path}')
    except FileNotFoundError:
        df.to_excel(file_path, index=False)  
        print(f'{file_path} created and data saved.')

def main():
    start_date = datetime.datetime.strptime('20241114', '%Y%m%d')
    end_date = datetime.datetime.strptime((start_date - datetime.timedelta(days=30)).strftime('%Y%m%d'), '%Y%m%d')
    output_file = f'{start_date.strftime("%Y%m%d")}_to_{end_date.strftime("%Y%m%d")}.xlsx'

    existing_bvids = load_existing_bvids("收录曲目.xlsx")
    
    get_all_videos(start_date, end_date, output_file, existing_bvids)

if __name__ == '__main__':
    main()
