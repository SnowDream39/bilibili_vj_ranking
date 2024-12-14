import asyncio
import pandas as pd
from math import ceil, floor
from datetime import datetime, timedelta

today = datetime.now().replace(hour=0, minute=0,second=0,microsecond=0).strftime('%Y%m%d')
old_time_toll = datetime.strptime(str(today), '%Y%m%d').strftime('%Y%m%d')
new_time_toll = (datetime.strptime(str(today), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')  
old_time_new = f'新曲{old_time_toll}'
new_time_new = f'新曲{new_time_toll}'

CONFIG = {
    "columns": [
        'title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer',
        'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin',
        'like', 'image_url'
    ],
    "threshold": 1000, # 新曲日增阈值
    "output_paths": {
        "data": "差异/非新曲",
        "new_song": "差异/新曲"
    }
}

def read_data(file_path, columns=None):
    return pd.read_excel(file_path, usecols=columns)

def calculate_scores(view, favorite, coin, like, copyright):
    ''' 公式 '''
    copyright = 1 if copyright in [1, 3] else 2
    fixA = 0 if coin <= 0 else (1 if copyright == 1 else ceil(max(1, (view + 20 * favorite + 40 * coin + 10 * like) / (200 * coin)) * 100) / 100)
    fixB = 0 if view + 20 * favorite <= 0 else ceil(min(1, 3 * max(0, (20 * coin + 10 * like)) / (view + 20 * favorite)) * 100) / 100
    fixC = 0 if like + favorite <= 0 else ceil(min(1, (like + favorite + 20 * coin * fixA)/(2 * like + 2 * favorite)) * 100) / 100
    
    viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 20 / view, 1) * 100) / 100, 0)
    favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 20 + view) * 40, 20) * 100) / 100, 0)
    coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 40 + view) * 80, 40) * 100) / 100, 0)
    likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)

    return viewR, favoriteR, coinR, likeR, fixA, fixB, fixC

def calculate_points(diff, scores):
    viewR, favoriteR, coinR, likeR = scores[:4]
    viewP = diff['view'] * viewR
    favoriteP = diff['favorite'] * favoriteR
    coinP = diff['coin'] * coinR * scores[4]
    likeP = diff['like'] * likeR
    return viewP + favoriteP + coinP + likeP

def process_records(records, old_data, new_data, data_type="data"):
    ''' 主体逻辑 '''
    collected_data = read_data('收录曲目.xlsx')
    
    result = []
    for i, record in records.iterrows():
        bvid = record.get("bvid")
        if not bvid:
            continue
        try:
            match = new_data['bvid'] == bvid
            if not match.any(): continue
            old_match = old_data['bvid'] == bvid
            new = new_data.loc[new_data['bvid'] == bvid].squeeze()
            if not old_match.any():  # 不处理后补充的旧曲
                if datetime.strptime(new['pubdate'], "%Y-%m-%d %H:%M:%S") < datetime.strptime(old_time_toll, "%Y%m%d"): continue
                else:  old = {'view': 0, 'favorite': 0, 'coin': 0, 'like': 0}
            else: old = old_data.loc[old_match].squeeze()
            if new.empty: continue

            if data_type == "new_song": # 用收录曲目的数据补充新曲数据
                collected_match = collected_data['bvid'] == bvid
                if collected_match.any():
                    collected_record = collected_data.loc[collected_match].squeeze()
                    for field in ['author', 'name', 'synthesizer', 'copyright', 'vocal', 'type']:
                        new[field] = collected_record.get(field, new[field])

            diff = {col: new[col] - old.get(col, 0) for col in ['view', 'favorite', 'coin', 'like']}
            scores = calculate_scores(diff['view'], diff['favorite'], diff['coin'], diff['like'], new['copyright'])
            point = round(scores[5] * scores[6] * calculate_points(diff, scores))

            result.append({
                'title': new['title'], 'bvid': bvid, 'name': new['name'], 'author': new['author'], 
                'uploader': new['uploader'], 'copyright': new['copyright'], 'synthesizer': new['synthesizer'], 
                'vocal': new['vocal'], 'type': new['type'], 'pubdate': new['pubdate'], 
                'duration': new['duration'], 'page': new['page'], 
                'view': diff['view'], 'favorite': diff['favorite'], 'coin': diff['coin'], 'like': diff['like'], 
                'viewR': f'{scores[0]:.2f}', 'favoriteR': f'{scores[1]:.2f}', 'coinR': f'{scores[2]:.2f}', 'likeR': f'{scores[3]:.2f}',
                'fixA': f'{scores[4]:.2f}', 'fixB': f'{scores[5]:.2f}', 'fixC': f'{scores[6]:.2f}',
                'point': point, 'image_url': new['image_url']
            })
        except Exception as e:
            print(f"Error processing record {bvid}: {e}")
    
    return pd.DataFrame(result)

def save_to_excel(df, filename):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    print(f"{filename} 保存完成")


def main_processing(old_data_path, new_data_path, output_path, point_threshold=None, data_type="data"):
    old_data = read_data(old_data_path, columns=CONFIG['columns'])
    new_data = read_data(new_data_path, columns=CONFIG['columns'])
    records = new_data if data_type == "new_song" else read_data('收录曲目.xlsx')

    df = process_records(records, old_data, new_data, data_type)
    if point_threshold:
        df = df[df['point'] >= point_threshold]
    df = df.sort_values('point', ascending=False)
    save_to_excel(df, output_path)


async def main():
    await asyncio.gather(
        asyncio.to_thread(main_processing, f'数据/{old_time_toll}.xlsx', f'数据/{new_time_toll}.xlsx', f"{CONFIG['output_paths']['data']}/{new_time_toll}与{old_time_toll}.xlsx"),
        asyncio.to_thread(main_processing, f'新曲数据/{old_time_new}.xlsx', f'新曲数据/{new_time_new}.xlsx', f"{CONFIG['output_paths']['new_song']}/{new_time_new}与{old_time_new}.xlsx", point_threshold=CONFIG['threshold'], data_type="new_song")
    )

if __name__ == "__main__":
    asyncio.run(main())
