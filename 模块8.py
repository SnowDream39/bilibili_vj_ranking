# 萌百条目
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta

index = 202410
id = 'BV111SXYdErh'
mode = 1
pubtime = 20241102 # for month ranking

ED_name ='怪电话'
ED_bvid ='BV1s44y1w7jk'
ED_pubdate ='2023-08-04 23:00:00'
ED_author ='r-906'
ED_image ='http://i0.hdslb.com/bfs/archive/03691a2052cdaf1b5e7e0e564585cf595c47812f.jpg'
ED_vocal ='ROSE、POPY'

def write_ed_song(text):
    text.write(f'{{{{虚拟歌手外语排行榜/bricks\n'
               f'|曲名={ED_name}\n|歌姬={ED_vocal}\n|P主={ED_author}\n|image link={ED_image}\n|本期=ED\n|color=#4FC1E9\n|bottom-column={{{{color|#4FC1E9|本期片尾}}}}\n|时间={ED_pubdate}\n|id={ED_bvid}}}}}\n')

def write_op_song(last_period_info, text):
    name, bvid, type, pubdate, author, image, vocal = (
        last_period_info['name'][0], 
        last_period_info['bvid'][0], 
        1 if (last_period_info['type'][0]=='翻唱') else '',
        last_period_info['pubdate'][0], 
        last_period_info['author'][0], 
        last_period_info['image_url'][0], 
        last_period_info['vocal'][0]
    )
    
    text.write(f'{{{{虚拟歌手外语排行榜/bricks\n'
               f'|曲名={name}\n|歌姬={vocal}\n|P主={author}\n|image link={image}\n|本期=OP\n|color=#AA0000\n|bottom-column={{{{color|#AA0000|上期冠军}}}}\n|时间={pubdate}\n|翻唱={type}\n|id={bvid}}}}}\n')
    
def get_last_week_data(name, last_week_info):
    match = last_week_info[last_week_info['name'] == name]
    if not match.empty:
        last_week_rank = match.iloc[0]['rank']
        last_week_point = match.iloc[0]['point']
        return last_week_rank, last_week_point
    return '', 'NEW'

def bricks(max_index, info, last_week_info, text):
    for i in info.index:
        if i >= max_index:
            break
        name = info.loc[i, 'name']
        pubdate = info.loc[i, 'pubdate']
        bvid = info.loc[i, 'bvid']
        image = info.loc[i, 'image_url']
        author = info.loc[i, 'author']
        vocal = info.loc[i, 'vocal']
        score = info.loc[i, 'point']
        view = info.loc[i, 'view']
        favorite = info.loc[i, 'favorite']
        coin = info.loc[i, 'coin']
        like = info.loc[i, 'like']
        viewR = info.loc[i, 'viewR']
        favoriteR = info.loc[i, 'favoriteR']
        coinR = info.loc[i, 'coinR']
        likeR = info.loc[i, 'likeR']
        rank = info.loc[i, 'rank']
        type = 1 if info.loc[i, 'type'] == '翻唱' else ''
        
        last_week_rank, last_week_point = get_last_week_data(name, last_week_info)
        rate = 'NEW' if last_week_point == 'NEW' else f"{round((score - last_week_point) / last_week_point * 100, 2):.2f}%"
       
        text.write(f'{{{{虚拟歌手外语排行榜/bricks\n|曲名={name}\n|歌姬={vocal}\n|P主={author}\n|image link={image}\n|本期={rank}\n|上期={last_week_rank}\n|时间={pubdate}\n|'+
                   f'翻唱={type}\n|得点={score}\n|rate={rate}\n|播放={view}\n|收藏={favorite}\n|硬币={coin}\n|点赞={like}\n|播放补正={viewR}\n|收藏补正={favoriteR}\n|硬币补正={coinR}\n|点赞补正={likeR}\n|id={bvid}\n}}}}\n')
        
        
def week_ranking(index, id):
    baseday = 20240831
    target_week = (datetime.strptime(str(baseday), "%Y%m%d") + timedelta(days=index * 7)).strftime("%Y%m%d")
    week = f'{target_week[:4]}-{target_week[4:6]}-{target_week[6:]}'
    last_week = (datetime.strptime(target_week, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d") 
    last_week_file = f'周刊/总榜/{last_week[:4]}-{last_week[4:6]}-{last_week[6:]}.xlsx'
    last_week_new_file = f'周刊/新曲榜/新曲{last_week[:4]}-{last_week[4:6]}-{last_week[6:]}.xlsx'

    info = pd.read_excel(f'周刊/总榜/{week}.xlsx')
    info_new = pd.read_excel(f'周刊/新曲榜/新曲{week}.xlsx')
    last_week_info = pd.read_excel(last_week_file)  
    last_week_info_new = pd.read_excel(last_week_new_file)  


    image = f'周刊虚拟歌手外语排行榜-{index}.jpg'

    text = open(f'萌百条目/{index}.txt', 'w', encoding='UTF-8')
    text.write(f'{{{{周刊虚拟歌手外语排行榜|index={index}|id={id}|image={image}}}}}' +
           f'\n\'\'\'术力口数据姬\'\'\'于{target_week[:4]}年{target_week[4:6]}月{target_week[6:]}日发布了\'\'\'周刊虚拟歌手外语排行榜 #{index}\'\'\'。\n\n' +
           f'==视频本体==\n{{{{BilibiliVideo|id={id}}}}}' +
           '\n==榜单==\n')
    write_op_song(last_week_info, text)
    text.write('\n===主榜===\n')
    bricks(20, info, last_week_info,text)
    text.write('\n===新曲榜===\n')
    bricks(10, info_new, last_week_info_new, text)
    write_ed_song(text)
    text.write('\n==杂谈==\n(待补充)\n{{虚拟歌手外语排行榜列表}}\n[[Category:周刊虚拟歌手外语排行榜]]')
    text.close()

def month_ranking(index, id, pubtime):
    month = f'{index[:4]}-{index[4:]}'
    last_month = (datetime.strptime(index, "%Y%m") + relativedelta(months=-1)).strftime("%Y%m")

    last_month_file = f'月刊/总榜/{last_month[:4]}-{last_month[4:]}.xlsx'
    last_month_new_file = f'月刊/新曲榜/新曲{last_month[:4]}-{last_month[4:]}.xlsx'

    info = pd.read_excel(f'月刊/总榜/{month}.xlsx')
    info_new = pd.read_excel(f'月刊/新曲榜/新曲{month}.xlsx')
    last_month_info = pd.read_excel(last_month_file)  
    last_month_info_new = pd.read_excel(last_month_new_file)  


    image = f'月刊虚拟歌手外语排行榜{index[:4]}{index[4:]}.jpg'

    text = open(f'萌百条目/月刊/{month}.txt', 'w', encoding='UTF-8')
    text.write(f'{{{{月刊虚拟歌手外语排行榜|index={index}|id={id}|image={image}|pubtime={pubtime}}}}}' +
           f'\n\'\'\'术力口数据姬\'\'\'于{pubtime[:4]}年{pubtime[4:6]}月{pubtime[6:]}日发布了\'\'\'月刊虚拟歌手外语排行榜 #{index[:4]}年{index[4:]}月\'\'\'。\n\n' +
           f'==视频本体==\n{{{{BilibiliVideo|id={id}}}}}' +
           '\n==榜单==\n')
    write_op_song(last_month_info, text)
    text.write('\n===主榜===\n')
    bricks(20, info,last_month_info,text)
    text.write('\n===新曲榜===\n')
    bricks(20, info_new, last_month_info_new, text)
    write_ed_song(text)
    text.write('\n==杂谈==\n(待补充)\n{{虚拟歌手外语排行榜列表}}\n[[Category:月刊虚拟歌手外语排行榜]]')
    text.close()
def main(index, id, mode, pubtime):
    if mode == 0:
        week_ranking(index, id)
    else:
        month_ranking(str(index), id, str(pubtime))

if __name__ == '__main__':
    main(index, id, mode, pubtime)