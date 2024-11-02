import asyncio
import pandas as pd
from math import ceil, floor
from datetime import datetime, timedelta

today = datetime.now().replace(hour=0, minute=0,second=0,microsecond=0)

old_time_toll = datetime.strptime(str(today), '%Y%m%d').strftime('%Y%m%d')
new_time_toll = (datetime.strptime(str(today), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
    
old_time_new = f'新曲{old_time_toll}'
new_time_new = f'新曲{new_time_toll}'


def read_data(file_path, columns=None):
    return pd.read_excel(file_path, usecols=columns)

def calculate_differences(new, old):
    return {col: new[col] - old.get(col, 0) for col in ['view', 'favorite', 'coin', 'like']}

def calculate_scores(view, favorite, coin, like, hascopyright):
    hascopyright = 1 if hascopyright in [1, 3] else 2
    fixA = 0 if coin <= 0 else (1 if hascopyright == 1 else ceil(max(1, (view + 20 * favorite + 40 * coin + 10 * like) / (200 * coin)) * 100) / 100)
    fixB = 0 if view + 20 * favorite <=0 else ceil(min(1, 3 * (20 * coin + 10 * like) / (view + 20 * favorite)) * 100) / 100
    fixC = 0 if like + favorite <= 0 else ceil(min(1, (like + favorite + 20 * coin * fixA)/(2 * like + 2 * favorite)) * 100) / 100
    
    viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 20 / view, 1) * 100) / 100, 0)
    favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 20 + view) * 40, 20) * 100) / 100, 0)
    coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin *40 + view) * 80, 40) * 100) / 100, 0)
    likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)

    return viewR, favoriteR, coinR, likeR, fixA, fixB, fixC


def format_scores(viewR, favoriteR, coinR, likeR,fixA, fixB, fixC):
    return f"{viewR:.2f}", f"{favoriteR:.2f}", f"{coinR:.2f}", f"{likeR:.2f}", f"{fixA:.2f}", f"{fixB:.2f}", f"{fixC:.2f}"

def calculate_points(view, favorite, coin, like, viewR, favoriteR, coinR, likeR):
    viewP = view * viewR
    favoriteP = favorite * favoriteR
    coinP = coin * coinR
    likeP = like * likeR
    return viewP + favoriteP + coinP + likeP

def process_records(records, old_data, new_data, data_type="data", collected_data=None):
    info_list = []
    for i in records.index:
        bvid = records.at[i, "BVID"] if data_type == "data" else records.at[i, "bvid"]
        pubdate = records.at[i, 'Pubdate'] if data_type == "data" else records.at[i, 'pubdate']
        if not bvid:
            continue
        try:
            new_record = new_data[new_data['bvid'] == bvid]
            old_record = old_data[old_data['bvid'] == bvid]
            
            if new_record.empty:
                continue

            new = new_record.iloc[0]
            if old_record.empty and data_type == "data":
                if datetime.strptime(pubdate, "%Y-%m-%d %H:%M:%S") < datetime.strptime(old_time_toll, "%Y%m%d"):
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
            page = new['page']
            synthesizer = new['synthesizer']
            vocal = new['vocal']
            type = new['type']
            image_url = new['image_url']
            
            if data_type == "new_song" and collected_data is not None:
                collected_record = collected_data[collected_data['BVID'] == bvid]
                if not collected_record.empty:
                    collected = collected_record.iloc[0]
                    name = collected['Title']
                    author = collected['Author']
                    hascopyright = collected['Copyright']
                    synthesizer = collected['Synthesizer']
                    vocal = collected['Vocal']
                    type = collected['Type']
                    image_url = new['image_url']
                    
            diff = calculate_differences(new, old)
            viewR, favoriteR, coinR, likeR , fixA, fixB, fixC = calculate_scores(diff['view'], diff['favorite'], diff['coin'], diff['like'], hascopyright)
            viewR, favoriteR, coinR, likeR, fixA, fixB, fixC = format_scores(viewR, favoriteR, coinR, likeR, fixA, fixB, fixC)
            point = round(float(fixB) * float(fixC) * calculate_points(diff['view'], diff['favorite'], diff['coin'] * float(fixA), diff['like'], float(viewR), float(favoriteR), float(coinR), float(likeR)))

            info_list.append([title, bvid, name, author, uploader, hascopyright, synthesizer, vocal, type, pubdate, duration, page, diff['view'], diff['favorite'], diff['coin'], diff['like'], viewR, favoriteR, coinR, likeR, fixA, fixB, fixC, point, image_url])
        
        except Exception as e:
            print(f"Error fetching info for BVID {bvid}: {e}")

    return info_list


def save_to_excel(df, filename, adjust_width=True):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        if adjust_width:
            worksheet = writer.sheets['Sheet1']
            for i, col in enumerate(df.columns, 1):
                max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.column_dimensions[worksheet.cell(row=1, column=i).column_letter].width = max_length


def main_processing(old_data_path, new_data_path, output_path, point_threshold=None, data_type="data"):
    columns = ['bvid', 'video_title', 'title', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin', 'like', 'image_url']
    old_data = read_data(old_data_path, columns=columns)
    new_data = read_data(new_data_path, columns=columns)

    collected_data = None
    if data_type == "new_song":
        collected_data = read_data('收录曲目.xlsx')

    if data_type == "data":
        records = read_data('收录曲目.xlsx')
    else:
        records = new_data

    info_list = process_records(records, old_data, new_data, data_type, collected_data)
    
    if info_list:
        stock_list = pd.DataFrame(info_list, columns=['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'fixA', 'fixB', 'fixC', 'point', 'image_url'])
        if point_threshold:
            stock_list = stock_list[stock_list['point'] >= point_threshold]
        stock_list = stock_list.sort_values('point', ascending=False)

        # 计算排名
        stock_list['view_rank'] = stock_list['view'].rank(ascending=False, method='min')
        stock_list['favorite_rank'] = stock_list['favorite'].rank(ascending=False, method='min')
        stock_list['coin_rank'] = stock_list['coin'].rank(ascending=False, method='min')
        stock_list['like_rank'] = stock_list['like'].rank(ascending=False, method='min')

        save_to_excel(stock_list, output_path)
        
        print(f"处理完成，已输出到{output_path}")


async def main() -> None:
    await asyncio.gather(
        
        asyncio.to_thread(main_processing, 
                          f'数据/{old_time_toll}.xlsx', 
                          f'数据/{new_time_toll}.xlsx', 
                          f"差异/非新曲/{new_time_toll}与{old_time_toll}.xlsx"),
        
        asyncio.to_thread(main_processing, 
                          f'新曲数据/{old_time_new}.xlsx', 
                          f'新曲数据/{new_time_new}.xlsx', 
                          f"差异/新曲/{new_time_new}与{old_time_new}.xlsx", 
                          point_threshold=1000, 
                          data_type="new_song")
    )

if __name__ == "__main__":
    asyncio.run(main())
