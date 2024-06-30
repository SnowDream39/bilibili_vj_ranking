import asyncio
import pandas as pd
from math import ceil, floor
from datetime import datetime
from openpyxl import Workbook

old_time = '20240630000302'
new_time = '20240630121343'

async def main() -> None:
    songs = pd.read_excel('收录曲目.xlsx')

    info_list = []  # 用于存储视频信息的列表
    simple_info_list = []
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

            title    = new['video_title'] #视频标题
            name     = new['title']       #通称曲目
            author   = new['author']        #作者
            uploder  = new['uploader']      #up主
            hascopyright = new['copyright'] #是否原创
            
            view     = new['view']     - old['view']
            favorite = new['favorite'] - old['favorite']
            coin     = new['coin']     - old['coin']
            share    = new['share']    - old['share']
            like     = new['like']     - old['like']
            reply    = new['reply']    - old['reply']
            danmaku  = new['danmaku']  - old['danmaku']

            # 添加除零检查并进行0.01级向上取整
            viewR = 0 if view == 0 else max(ceil(min(max((coin + favorite + like), 0) * 25 / view, 1) * 100) / 100, 0)
            favoriteR = 0 if favorite * 20 + view <= 0 else max(ceil(min(max(favorite, 0) * 20 / (favorite * 20 + view) * 40, 20) * 100) / 100, 0)
            coinR = 0 if coin == 0 else max(ceil(min((coin * 100 + view) / (coin * 100) * 10, 40) * 100) / 100, 0)
            likeR = 0 if like * 20 + view <= 0 else max(floor(max(coin + favorite, 0) / (like * 20 + view) * 100 * 100) / 100, 0)

            viewP = view * viewR
            favoriteP = favorite * favoriteR
            coinP = coin * coinR
            likeP = like * likeR
            point = viewP + favoriteP + coinP + likeP
            
            # 强制两位小数输出
            viewR = f"{viewR:.2f}"
            favoriteR = f"{favoriteR:.2f}"
            coinR = f"{coinR:.2f}"
            likeR = f"{likeR:.2f}"

            # 四舍五入到整数
            info_list.append([title, bvid, name, author, uploder, hascopyright, pubdate, view, favorite, coin, like, round(viewP), viewR, round(favoriteP), favoriteR, round(coinP), coinR, round(likeP), likeR, round(point)])
            simple_info_list.append([bvid, name, author, pubdate, view, favorite, coin, like, round(viewP), round(favoriteP), round(coinP), round(likeP), round(point)])
        except Exception as e:
            print(f"Error fetching info for BVID {bvid}: {e}")

    # 将详细信息列表转换为Pandas DataFrame并计算排名
    if info_list:  # 确保info_list不为空
        stock_list = pd.DataFrame(info_list, columns=['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'pubdate', 'view', 'favorite', 'coin', 'like', 'viewP', 'viewR', 'favoriteP','favoriteR','coinP', 'coinR', 'likeP', 'likeR', 'point'])
        stock_list = stock_list.sort_values('point', ascending=False)

        # 计算排名
        stock_list['view_rank'] = stock_list['view'].rank(ascending=False, method='min')
        stock_list['favorite_rank'] = stock_list['favorite'].rank(ascending=False, method='min')
        stock_list['coin_rank'] = stock_list['coin'].rank(ascending=False, method='min')
        stock_list['like_rank'] = stock_list['like'].rank(ascending=False, method='min')

        # 保存详细信息为Excel文件并自动调整列宽
        filename = f"差异/{new_time}与{old_time}.xlsx"
        writer = pd.ExcelWriter(filename, engine='openpyxl')
        stock_list.to_excel(writer, index=False, sheet_name='Sheet1')
        worksheet = writer.sheets['Sheet1']
        for i, column_cells in enumerate(worksheet.columns):
            length = max(len(str(cell.value)) for cell in column_cells)
            worksheet.column_dimensions[worksheet.cell(row=1, column=i+1).column_letter].width = length + 2
        writer.close()  # 关闭写入器对象
        print("处理完成，详细数据已保存到", filename)

    # 将简化信息列表转换为Pandas DataFrame并保存为Excel文件
    if simple_info_list:  # 确保simple_info_list不为空
        simple_stock_list = pd.DataFrame(simple_info_list, columns=['bvid', 'name', 'author', 'pubdate', 'view', 'favorite', 'coin', 'like', 'viewP', 'favoriteP', 'coinP', 'likeP', 'point'])
        simple_stock_list = simple_stock_list.sort_values('point', ascending=False)

        # 保存简化信息为Excel文件并自动调整列宽
        simple_filename = f"差异/simple/{new_time}与{old_time}.xlsx"
        simple_writer = pd.ExcelWriter(simple_filename, engine='openpyxl')
        simple_stock_list.to_excel(simple_writer, index=False, sheet_name='Sheet1')
        simple_worksheet = simple_writer.sheets['Sheet1']
        for i, column_cells in enumerate(simple_worksheet.columns):
            length = max(len(str(cell.value)) for cell in column_cells)
            simple_worksheet.column_dimensions[simple_worksheet.cell(row=1, column=i+1).column_letter].width = length + 2
        simple_writer.close()  # 关闭写入器对象
        print("处理完成，简化数据已保存到", simple_filename)

if __name__ == "__main__":
    asyncio.run(main())
