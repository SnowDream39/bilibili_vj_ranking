import pandas as pd
from math import ceil, floor
from datetime import datetime
from openpyxl import Workbook

song_data = '总榜'

def read_data(file_path, columns=None):
    return pd.read_excel(file_path, usecols=columns)


def calculate_scores(view, favorite, coin, like, hascopyright):
    viewR = 0 if view == 0 else max(ceil(min(max((coin + favorite), 0) * 20 / view, 1) * 100) / 100, 0)
    favoriteR = 0 if favorite <= 0 else max(ceil(min(max(favorite + 2 * coin, 0) * 10 / (favorite * 20 + view) * 40, 20) * 100) / 100, 0)
    coinR = 0 if (1 if hascopyright in [1, 3] else 2) * coin * 40 + view == 0 else max(ceil(min(((1 if hascopyright in [1, 3] else 2) * coin * 40) / ((1 if hascopyright in [1, 3] else 2) * coin * 40 + view) * 80, 40) * 100) / 100, 0)
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


def process_records(new_data):
    info_list = []
    for i in new_data.index:
        bvid = new_data.at[i, "bvid"]
        pubdate = new_data.at[i, 'pubdate']
        if not bvid:
            continue
        try:
            new = new_data.iloc[i]
            title = new['video_title']
            name = new['title']
            author = new['author']
            uploader = new['uploader']
            hascopyright = new['copyright']
            duration = new['duration']
            synthesizer = new['synthesizer']
            vocal = new['vocal']

            diff = {'view': new['view'], 'favorite': new['favorite'], 'coin': new['coin'], 'like': new['like']}
            viewR, favoriteR, coinR, likeR = calculate_scores(diff['view'], diff['favorite'], diff['coin'], diff['like'], hascopyright)
            viewR, favoriteR, coinR, likeR = format_scores(viewR, favoriteR, coinR, likeR)
            point = calculate_points(diff['view'], diff['favorite'], diff['coin'], diff['like'], float(viewR), float(favoriteR), float(coinR), float(likeR))

            info_list.append([title, bvid, name, author, uploader, hascopyright, synthesizer, vocal, pubdate, duration, diff['view'], diff['favorite'], diff['coin'], diff['like'], viewR, favoriteR, coinR, likeR, point])
        
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


def main_processing(data_path, output_path, point_threshold=None):
    columns = ['bvid', 'video_title', 'title', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like']
    new_data = read_data(data_path, columns=columns)

    info_list = process_records(new_data)
    
    if info_list:
        stock_list = pd.DataFrame(info_list, columns=['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point'])
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


if __name__ == "__main__":
    main_processing(f'特殊/特殊原始数据/{song_data}.xlsx', f"特殊/特殊排行榜/{song_data}.xlsx")
