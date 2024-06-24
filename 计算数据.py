import asyncio
import pandas as pd
from math import ceil
from datetime import datetime

old_time = '20240624000503'
new_time = '20240624120425'

async def main() -> None:
    songs = pd.read_excel('收录曲目.xlsx')

    info_list = []  # 用于存储视频信息的列表
    old_data = pd.read_excel(f'数据/{old_time}.xlsx')
    new_data = pd.read_excel(f'数据/{new_time}.xlsx')
    
    for i in songs.index:
        bvid = songs.at[i, "BVID"]
        pubdate = songs.at[i, 'Pubdate']
        if not bvid:
            continue
        try:
            new_record = new_data[new_data['bvid'] == bvid]
            old_record = old_data[old_data['bvid'] == bvid]
            
            if new_record.empty:
                continue
            else: 
                new = new_record.iloc[0]
            if old_record.empty: 
                if datetime.strptime(pubdate, "%Y-%m-%d %H:%M:%S") < datetime.strptime(old_time, "%Y%m%d%H%M%S"):
                    continue
                else:
                    old = {'view':0, 'favorite':0, 'coin':0, 'share':0, 'like':0, 'reply':0, 'danmaku':0}
            else: 
                old = old_record.iloc[0]

            name     = new['title']
            view     = new['view']     - old['view']
            favorite = new['favorite'] - old['favorite']
            coin     = new['coin']     - old['coin']
            share    = new['share']    - old['share']
            like     = new['like']     - old['like']
            reply    = new['reply']    - old['reply']
            danmaku  = new['danmaku']  - old['danmaku']

            # 添加除零检查并进行0.01级向上取整
            viewR = 0 if view == 0 else max(ceil(view * (min((coin + favorite + like) * 25 / view, 1) * 100)) / 100, 0)
            favoriteR = 0 if favorite * 20 + view == 0 else max(ceil(favorite * (min(favorite * 20 / (favorite * 20 + view) * 40, 20)) * 100) / 100, 0)
            coinR = 0 if coin * 100 == 0 else max(ceil(coin * (min((coin * 100 + view) / (coin * 100) * 10, 40)) * 100) / 100, 0)
            likeR = 0 if like * 20 + view == 0 else max(ceil(like * (coin + favorite) / (like * 20 + view) * 100 * 100) / 100, 0)

            point = viewR + favoriteR + coinR + likeR
            # 四舍五入到整数
            info_list.append([bvid, name, pubdate, view, favorite, coin, share, like, round(viewR), round(favoriteR), round(coinR), round(likeR), round(point)])
        
        except Exception as e:
            print(f"Error fetching info for BVID {bvid}: {e}")

    # 将列表转换为Pandas DataFrame并保存为Excel文件
    if info_list:  # 确保info_list不为空
        stock_list = pd.DataFrame(info_list, columns=['bvid', 'name', 'pubdate', 'view', 'favorite', 'coin', 'share', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point'])
        stock_list = stock_list.sort_values('point', ascending=False)
        filename = f"差异/{new_time}与{old_time}.xlsx"
        stock_list.to_excel(filename, index=False)
        print("处理完成，数据已保存到", filename)

if __name__ == "__main__":
    asyncio.run(main())
