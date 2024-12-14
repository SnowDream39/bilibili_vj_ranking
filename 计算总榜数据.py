import asyncio
import pandas as pd
from math import ceil, floor

song_data = '催眠者'

CONFIG = {
    "columns": [
        'title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer',
        'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin',
        'like', 'image_url'
    ],
    "paths": {
        "input_path": f"特殊/特殊原始数据/{song_data}.xlsx",
        "output_path": f"特殊/特殊排行榜/{song_data}.xlsx"
    }
}

def read_data(file_path, columns=None):
    return pd.read_excel(file_path, usecols=columns)

def calculate_scores(view, favorite, coin, like, copyright):
    copyright = 1 if copyright in [1, 3] else 2
    fixA = 0 if coin <= 0 else (1 if copyright == 1 else ceil(max(1, (view + 20 * favorite + 40 * coin + 10 * like) / (200 * coin)) * 100) / 100)
    fixB = 0 if view + 20 * favorite <= 0 else ceil(min(1, 3 * (20 * coin + 10 * like) / (view + 20 * favorite)) * 100) / 100
    fixC = 0 if like + favorite <= 0 else ceil(min(1, (like + favorite + 20 * coin * fixA)/(2 * like + 2 * favorite)) * 100) / 100
    
    viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 30 / view, 1) * 100) / 100, 0)
    favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 10 + view) * 40, 20) * 100) / 100, 0)
    coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 20 + view) * 80, 40) * 100) / 100, 0)
    likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)

    return viewR, favoriteR, coinR, likeR, fixA, fixB, fixC

def calculate_points(data, scores):
    viewR, favoriteR, coinR, likeR = scores[:4]
    viewP = data['view'] * viewR
    favoriteP = data['favorite'] * favoriteR
    coinP = data['coin'] * coinR * scores[4]
    likeP = data['like'] * likeR
    return viewP + favoriteP + coinP + likeP

def calculate_ranks(df):
    """重新计算各项排名"""
    df = df.sort_values('point', ascending=False)
    for col in ['view', 'favorite', 'coin', 'like']:
        df[f'{col}_rank'] = df[col].rank(ascending=False, method='min')
    df['rank'] = df['point'].rank(ascending=False, method='min')
    return df

def process_records(new_data):
    ''' 主体逻辑 '''
    result = []
    for _, record in new_data.iterrows():
        bvid = record.get("bvid")
        if not bvid:
            continue
        try:
            data = {'view': record['view'], 'favorite': record['favorite'], 'coin': record['coin'], 'like': record['like']}
            scores = calculate_scores(data['view'], data['favorite'], data['coin'], data['like'], record['copyright'])
            point = round(scores[5] * scores[6] * calculate_points(data, scores))

            result.append({
                'title': record['title'], 'bvid': bvid, 'name': record['name'], 'author': record['author'], 'uploader': record['uploader'],
                'copyright': record['copyright'], 'synthesizer': record['synthesizer'], 'vocal': record['vocal'], 'type': record['type'],
                'pubdate': record['pubdate'], 'duration': record['duration'], 'page': record['page'],
                'view': data['view'], 'favorite': data['favorite'], 'coin': data['coin'], 'like': data['like'],
                'viewR': f'{scores[0]:.2f}', 'favoriteR': f'{scores[1]:.2f}', 'coinR': f'{scores[2]:.2f}', 'likeR': f'{scores[3]:.2f}',
                'fixA': f'{scores[4]:.2f}', 'fixB': f'{scores[5]:.2f}', 'fixC': f'{scores[6]:.2f}', 'point': point, 'image_url': record['image_url']
            })
        except Exception as e:
            print(f"Error processing record {bvid}: {e}")
    
    return pd.DataFrame(result)

def save_to_excel(df, filename):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    print(f"{filename} 保存完成")

def main_processing(input_path, output_path):
    data = read_data(input_path, columns=CONFIG['columns'])
    df = process_records(data)  
    df = calculate_ranks(df)
    save_to_excel(df, output_path)

if __name__ == "__main__":
    main_processing(CONFIG["paths"]["input_path"],CONFIG["paths"]["output_path"])
