import asyncio
import pandas as pd
from math import ceil, floor
from datetime import datetime
from openpyxl import Workbook

old_time_data = '20240901'
new_time_data = '20241001'
target_month = '2024-09'  # 目标月份

def read_data(file_path, columns=None):
    return pd.read_excel(file_path, usecols=columns)

def calculate_differences(new, old):
    return {col: new[col] - old.get(col, 0) for col in ['view', 'favorite', 'coin', 'like']}

def calculate_scores(view, favorite, coin, like, hascopyright):
    viewR = 0 if view == 0 else max(ceil(min(max((coin + favorite), 0) * 25 / view, 1) * 100) / 100, 0)
    favoriteR = 0 if favorite <= 0 else max(ceil(min(max(favorite + 2 * coin, 0) * 10 / (favorite * 15 + view) * 30, 20) * 100) / 100, 0)
    coinR = 0 if (1 if hascopyright in [1, 3] else 2) * coin * 40 + view == 0 else max(ceil(min(((1 if hascopyright in [1, 3] else 2) * coin * 40) / ((1 if hascopyright in [1, 3] else 2) * coin * 30 + view) * 80, 40) * 100) / 100, 0)
    likeR = 0 if like <= 0 else max(floor(max(coin + favorite, 0) / (like * 20 + view) * 100 * 100) / 100, 0)

    return viewR, favoriteR, coinR, likeR

def format_scores(viewR, favoriteR, coinR, likeR):
    return f"{viewR:.2f}", f"{favoriteR:.2f}", f"{coinR:.2f}", f"{likeR:.2f}"

def calculate_points(view, favorite, coin, like, viewR, favoriteR, coinR, likeR):
    viewP = view * viewR
    favoriteP = favorite * favoriteR
    coinP = coin * coinR
    likeP = like * likeR
    return round(viewP + favoriteP + coinP + likeP)

def process_records(records, old_data, new_data):
    info_list = []
    for i in records.index:
        bvid = records.at[i, "bvid"]
        pubdate = str(records.at[i, 'pubdate'])
        if not bvid:
            continue
        try:
            new_record = new_data[new_data['bvid'] == bvid]
            old_record = old_data[old_data['bvid'] == bvid]
            
            if new_record.empty:
                continue

            new = new_record.iloc[0]
            if old_record.empty:
                if datetime.strptime(pubdate, "%Y-%m-%d %H:%M:%S") < datetime.strptime(old_time_data, "%Y%m%d"):
                    continue
                old = {'view': 0, 'favorite': 0, 'coin': 0, 'like': 0}
            else:
                old = old_record.iloc[0] if not old_record.empty else {'view': 0, 'favorite': 0, 'coin': 0, 'like': 0}
            
            title = new['video_title']
            name = new['title']
            author = new['author']
            uploader = new['uploader']
            hascopyright = new['copyright']
            duration = new['duration']
            synthesizer = new['synthesizer']
            vocal = new['vocal']
            type = new['type']
            image_url = new['image_url']

            diff = calculate_differences(new, old)
            viewR, favoriteR, coinR, likeR = calculate_scores(diff['view'], diff['favorite'], diff['coin'], diff['like'], hascopyright)
            viewR, favoriteR, coinR, likeR = format_scores(viewR, favoriteR, coinR, likeR)
            point = calculate_points(diff['view'], diff['favorite'], diff['coin'], diff['like'], float(viewR), float(favoriteR), float(coinR), float(likeR))

            info_list.append([title, bvid, name, author, uploader, hascopyright, synthesizer, vocal, type, pubdate, duration, diff['view'], diff['favorite'], diff['coin'], diff['like'], viewR, favoriteR, coinR, likeR, point, image_url])
        
        except Exception as e:
            print(f"Error fetching info for bvid {bvid}: {e}")

    return info_list

def save_to_excel(df, filename, adjust_width=True):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        if adjust_width:
            worksheet = writer.sheets['Sheet1']
            for i, col in enumerate(df.columns, 1):
                max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.column_dimensions[worksheet.cell(row=1, column=i).column_letter].width = max_length

def filter_new_songs(info_list, top_20_bvids):
    new_songs_list = []
    for record in info_list:
        pubdate = str(record[9])
        bvid = record[1]
        if target_month in pubdate and bvid not in top_20_bvids:
            new_songs_list.append(record)
    
    new_songs_df = pd.DataFrame(new_songs_list, columns=['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point', 'image_url'])
    new_songs_df = new_songs_df.sort_values('point', ascending=False)
    
    # 计算排名
    new_songs_df['view_rank'] = new_songs_df['view'].rank(ascending=False, method='min')
    new_songs_df['favorite_rank'] = new_songs_df['favorite'].rank(ascending=False, method='min')
    new_songs_df['coin_rank'] = new_songs_df['coin'].rank(ascending=False, method='min')
    new_songs_df['like_rank'] = new_songs_df['like'].rank(ascending=False, method='min')
    new_songs_df['rank'] = new_songs_df['point'].rank(ascending=False, method='min')
    
    return new_songs_df

def main_processing(old_data_path, new_data_path, output_path, new_songs_output_path):
    columns = ['bvid', 'video_title', 'title', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like', 'image_url']
    old_data = read_data(old_data_path, columns=columns)
    new_data = read_data(new_data_path, columns=columns)

    records = new_data

    info_list = process_records(records, old_data, new_data)
    
    if info_list:
        # 处理总榜
        stock_list = pd.DataFrame(info_list, columns=['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point', 'image_url'])
        stock_list = stock_list.sort_values('point', ascending=False)

        # 计算总榜排名
        stock_list['view_rank'] = stock_list['view'].rank(ascending=False, method='min')
        stock_list['favorite_rank'] = stock_list['favorite'].rank(ascending=False, method='min')
        stock_list['coin_rank'] = stock_list['coin'].rank(ascending=False, method='min')
        stock_list['like_rank'] = stock_list['like'].rank(ascending=False, method='min')
        stock_list['rank'] = stock_list['point'].rank(ascending=False, method='min')
      
        save_to_excel(stock_list, output_path)
       
        # 处理新曲榜
        top_20_bvids = stock_list.head(20)['bvid'].tolist()
        new_songs_df = filter_new_songs(info_list, top_20_bvids)
 
        if not new_songs_df.empty:
            save_to_excel(new_songs_df, new_songs_output_path)
            print(f"处理完成，新曲榜已输出到 {new_songs_output_path}")

        print(f"处理完成，已输出到{output_path}")

async def main() -> None:
    await asyncio.gather(
        asyncio.to_thread(main_processing, 
                          f'数据/{old_time_data}.xlsx', 
                          f'数据/{new_time_data}.xlsx', 
                          f"月榜/总榜/{target_month}.xlsx",
                          f"月榜/新曲榜/新曲{target_month}.xlsx")
    )

if __name__ == "__main__":
    asyncio.run(main())
