import asyncio
import pandas as pd
from math import ceil
from datetime import datetime

old_file = '20240624000503'
new_file = '20240624120425'

async def main() -> None:
    songs = pd.read_excel('收录曲目.xlsx')
    bv = songs.BVID

    info_list = []  # 用于存储视频信息的列表
    old_data = pd.read_excel(f'数据/{old_file}.xlsx')
    new_data = pd.read_excel(f'数据/{new_file}.xlsx')
    
    for bvid in bv:
        if not bvid:
            continue
        try:
            new_record = new_data[new_data['bvid'] == bvid]
            old_record = old_data[old_data['bvid'] == bvid]
            
            if not new_record.empty:
                new = new_record.iloc[0]
            else:
                new = None

            if not old_record.empty:
                old = old_record.iloc[0]
            else:
                old = None

            name = new['title'] if new is not None else (old['title'] if old is not None else "Unknown")
            
            view = (new['view'] if new is not None else 0) - (old['view'] if old is not None else 0)
            favorite = (new['favorite'] if new is not None else 0) - (old['favorite'] if old is not None else 0)
            coin = (new['coin'] if new is not None else 0) - (old['coin'] if old is not None else 0)
            share = (new['share'] if new is not None else 0) - (old['share'] if old is not None else 0)
            like = (new['like'] if new is not None else 0) - (old['like'] if old is not None else 0)
            danmaku = (new['danmaku'] if new is not None else 0) - (old['danmaku'] if old is not None else 0)

            # 添加除零检查并进行0.01级向上取整
            if view == 0:
                viewR = 0
            else:
                viewR = max(ceil(view * (min((coin + favorite + like) * 25 / view, 1) * 100)) / 100, 0)
            
            if favorite * 20 + view == 0:
                favoriteR = 0
            else:
                favoriteR = max(ceil(favorite * (min(favorite * 20 / (favorite * 20 + view) * 40, 20)) * 100) / 100, 0)
            
            if coin * 100 == 0:
                coinR = 0
            else:
                coinR = max(ceil(coin * (min((coin * 100 + view) / (coin * 100) * 10, 40)) * 100) / 100, 0)
            
            if like * 20 + view == 0:
                likeR = 0
            else:
                likeR = max(ceil(like * (coin + favorite) / (like * 20 + view) * 100 * 100) / 100, 0)
            
            point = viewR + favoriteR + coinR + likeR
            # 四舍五入到整数
            info_list.append([bvid, name, view, favorite, coin, share, like, round(viewR), round(favoriteR), round(coinR), round(likeR), round(point)])
        
        except Exception as e:
            print(f"Error fetching info for BVID {bvid}: {e}")

    # 将列表转换为Pandas DataFrame并保存为Excel文件
    if info_list:  # 确保info_list不为空
        stock_list = pd.DataFrame(info_list, columns=['bvid', 'name', 'view', 'favorite', 'coin', 'share', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point'])
        stock_list = stock_list.sort_values('point', ascending=False)
        filename = f"差异/{new_file}与{old_file}.xlsx"
        stock_list.to_excel(filename, index=False)
        print("处理完成，数据已保存到", filename)

if __name__ == "__main__":
    asyncio.run(main())
