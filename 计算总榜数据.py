# 根据总体数据计算生成榜单
import pandas as pd
from math import ceil, floor

song_data = '梦的结唱2-原创'

def read_data(file_path):
    return pd.read_excel(file_path)

def calculate_scores(view, favorite, coin, like, hascopyright):
    hascopyright = 1 if hascopyright in [1, 3] else 2
    fixA = 0 if coin <= 0 else (1 if hascopyright == 1 else ceil(max(1, (view + 20 * favorite + 40 * coin + 10 * like) / (200 * coin)) * 100) / 100)
    fixB = 0 if view + 20 * favorite <=0 else ceil(min(1, 3 * (20 * coin + 10 * like) / (view + 20 * favorite)) * 100) / 100
    fixC = 0 if like + favorite <= 0 else ceil(min(1, (like + favorite + 20 * coin * fixA)/(2 * like + 2 * favorite)) * 100) / 100
    
    viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 15 / view, 1) * 100) / 100, 0)
    favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 15 + view) * 40, 20) * 100) / 100, 0)
    coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 30 + view) * 80, 40) * 100) / 100, 0)
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

def process_records(new_data) -> pd.DataFrame:
    # 输出文件的列，需与下面的info的键保持一致
    columns = ['title','bvid','name','author','uploader','copyright','synthesizer','vocal','type','pubdate','duration','page','view','favorite','coin','like','viewR','favoriteR','coinR','likeR','fixA','fixB','fixC','point','image_url','view_rank','favorite_rank','coin_rank','like_rank','rank']
    stock_list = pd.DataFrame(columns=columns)
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
            page = new['page']
            image_url = new['image_url']
            synthesizer = new['synthesizer']
            vocal = new['vocal']
            type = new['type']

            diff = {'view': new['view'], 'favorite': new['favorite'], 'coin': new['coin'], 'like': new['like']}
            viewR, favoriteR, coinR, likeR , fixA, fixB, fixC = calculate_scores(diff['view'], diff['favorite'], diff['coin'], diff['like'], hascopyright)
            viewR, favoriteR, coinR, likeR, fixA, fixB, fixC = format_scores(viewR, favoriteR, coinR, likeR, fixA, fixB, fixC)
            point = round(float(fixB) * float(fixC) * calculate_points(diff['view'], diff['favorite'], diff['coin'] * float(fixA), diff['like'], float(viewR), float(favoriteR), float(coinR), float(likeR)))

            info =  {'title':title, 'bvid':bvid, 'name':name, 'author':author, 'uploader':uploader, 'copyright':hascopyright, 
                    'synthesizer':synthesizer, 'vocal':vocal, 'type':type, 'pubdate':pubdate, 'duration':duration, 'page':page,
                    'view':diff['view'], 'favorite':diff['favorite'], 'coin':diff['coin'], 'like':diff['like'],
                    'viewR':viewR, 'favoriteR':favoriteR, 'coinR':coinR, 'likeR':likeR,
                    'fixA':fixA, 'fixB':fixB, 'fixC':fixC, 'point':point, 'image_url':image_url}
            info = pd.DataFrame([info])
            stock_list = pd.concat([stock_list, info])
        
        except Exception as e:
            print(f"Error fetching info for BVID {bvid}: {e}")

    return stock_list


def save_to_excel(df, filename, adjust_width=True):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        if adjust_width:
            worksheet = writer.sheets['Sheet1']
            for i, col in enumerate(df.columns, 1):
                col_name = str(col)
                max_length = max(df[col].astype(str).map(len).max(), len(col_name)) + 2
                worksheet.column_dimensions[worksheet.cell(row=1, column=i).column_letter].width = max_length

def main_processing(data_path, output_path, point_threshold=None):
    new_data = read_data(data_path)

    stock_list = process_records(new_data)
    
    if not stock_list.empty:
        if point_threshold:
            stock_list = stock_list[stock_list['point'] >= point_threshold]
        stock_list = stock_list.sort_values('point', ascending=False)

        # 计算排名
        stock_list['view_rank'] = stock_list['view'].rank(ascending=False, method='min')
        stock_list['favorite_rank'] = stock_list['favorite'].rank(ascending=False, method='min')
        stock_list['coin_rank'] = stock_list['coin'].rank(ascending=False, method='min')
        stock_list['like_rank'] = stock_list['like'].rank(ascending=False, method='min')
        stock_list['rank'] = stock_list['point'].rank(ascending=False, method='min')
        save_to_excel(stock_list, output_path)
        
        print(f"处理完成，已输出到{output_path}")


if __name__ == "__main__":
    main_processing(f'特殊/特殊原始数据/{song_data}.xlsx', f"特殊/特殊排行榜/{song_data}.xlsx")
