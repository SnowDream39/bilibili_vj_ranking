import asyncio  
import pandas as pd  
from datetime import datetime  
from bilibili_api import video  
import time

songs = pd.read_excel('收录曲目.xlsx')
info_list = []
error_list = []
data_list= []

async def get_song_stat(i):
    global info_list
    title = songs.at[i, 'Title']
    bvid = songs.at[i, 'BVID']
    video_title = songs.at[i, 'Video Title']
    pubdate = songs.at[i, 'Pubdate']
    
    v = video.Video(bvid=bvid)
    info = await v.get_info()  
    stat_data = info.get('stat')  # 提取stat字段的数据  
    if stat_data:  
        view = stat_data.get('view')  # 观看数  
        favorite = stat_data.get('favorite')  # 收藏数  
        coin = stat_data.get('coin')  # 投币数  
        share = stat_data.get('share')  # 分享数  
        like = stat_data.get('like')  # 点赞数
        reply = stat_data.get('reply')
        danmaku = stat_data.get('danmaku')
        
        # 其他你可能需要的字段，如danmaku, reply等  
        print(title, view)
        
        # 确保所有值都存在，否则跳过该视频  
        info_list.append([bvid, title, pubdate, view, danmaku, reply, favorite, coin, share, like])  
        data_list.append([title, bvid, video_title, view, pubdate])

async def main() -> None:  
    for i in songs.index:
        time.sleep(0.3)
        try:
            await get_song_stat(i)
        except Exception:
            error_list.append(i)

    for i in error_list:
        time.sleep(0.3)
        try:
            await get_song_stat(i)
            error_list.remove(i)
        except Exception:
            pass
    print(*error_list)

    global info_list
  
    # 将列表转换为Pandas DataFrame并保存为Excel文件  
    if info_list:  # 确保info_list不为空  
        stock_list = pd.DataFrame(info_list, columns=['bvid', 'title', 'pubdate', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])  
        filename = "数据/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".xlsx"  
        stock_list.to_excel(filename, index=False)  
        print("处理完成，数据已保存到", filename)  
        
    if data_list:
        stock_list = pd.DataFrame(data_list, columns=['Title', 'BVID', 'Video Title', 'View', 'Pubdate'])
        stock_list = stock_list.sort_values(by='View', ascending=False)
        stock_list.to_excel('收录曲目.xlsx', index=False)
        print("收录曲目已更新并按观看数排序")
    else:  
        print("没有可用的视频信息，未保存数据")  
  
if __name__ == "__main__":  
    asyncio.run(main())
