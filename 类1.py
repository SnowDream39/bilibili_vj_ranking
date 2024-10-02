#普查全区

import requests
import pandas as pd
import datetime
import time
import re
from openpyxl import load_workbook

# 正则表达式移除非法字符
def remove_illegal_characters(text):
    # Excel 不支持某些控制字符，我们使用正则表达式移除它们
    return re.sub(r'[\x00-\x1F\x7F]', '', text)

def get_videos(time_from, time_to, max_retries=5):
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
            time.sleep(2 ** retries)  # Exponential backoff
            continue
        
        data = response.json()
        
        if 'data' in data and 'result' in data['data']:
            videos = data['data']['result']
            if not videos:
                print(f"No videos found in this page. Retrying...")
                retries += 1
                time.sleep(2 ** retries)  # Exponential backoff
                continue

            retries = 0  # Reset retries on success
            print(f"Found {len(videos)} videos in this page.")
            for video in videos:
                try:
                    view_count = int(video['play'])
                    if view_count < 10000:
                        print(f"Found a video with less than 100,000 views: {video['title']}, View: {view_count}")
                        return all_videos  # Jump to the next time period
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

# Function to get data from multiple time ranges
def get_all_videos(start_date, end_date, output_file):
    all_videos = []
    current_date = start_date
    
    while current_date >= end_date:
        next_date = max(current_date - datetime.timedelta(days=90), end_date)
        time_from = next_date.strftime('%Y%m%d')
        time_to = current_date.strftime('%Y%m%d')
        
        videos = get_videos(time_from, time_to)
        if videos:
            all_videos.extend(videos)
            # Append the data to the Excel file
            df = pd.DataFrame(videos)
            append_to_excel(df, output_file)

        current_date = next_date - datetime.timedelta(days=1)  # Move to the day before next_date
    
    return all_videos

# Function to append DataFrame to an Excel file
def append_to_excel(df, file_path):
    try:
        with pd.ExcelWriter(file_path, mode='a', if_sheet_exists='overlay', engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=writer.sheets is None, startrow=writer.sheets['Sheet1'].max_row if 'Sheet1' in writer.sheets else 0)
            print(f'Data appended to {file_path}')
    except FileNotFoundError:
        df.to_excel(file_path, index=False)  # If file doesn't exist, create a new one
        print(f'{file_path} created and data saved.')

# Main function
def main():
    start_date = datetime.datetime.strptime('20240911', '%Y%m%d')
    end_date = datetime.datetime.strptime('20090901', '%Y%m%d')
    output_file = 'bilibili_vocaloid_videos.xlsx'
    
    get_all_videos(start_date, end_date, output_file)

if __name__ == '__main__':
    main()
