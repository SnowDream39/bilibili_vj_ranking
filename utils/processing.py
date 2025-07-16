# utils/processing.py
# 数据处理模块：视频数据的清洗、合并和评分计算
import pandas as pd
from datetime import datetime
from utils.calculator import calculate

def process_records(
    new_data,
    old_data=None,
    use_old_data=False,
    use_collected=False,
    collected_data=None,
    ranking_type='daily',
    old_time_toll=None
):
    """
    视频数据处理主函数

    处理流程:
    1. 遍历所有视频记录
    2. 对每个视频:
       - 获取BV号
       - 获取新数据
       - 匹配旧数据(如果需要)
       - 补充收录数据(如果有)
       - 计算分数
    3. 生成处理后的DataFrame

    参数:
        new_data (pd.DataFrame): 新获取的视频数据
        old_data (pd.DataFrame, optional): 上期数据,用于计算增量（若为None则为总量）
        use_old_data (bool): 是否使用上期数据对比
        use_collected (bool): 是否是部分打标的新曲
        collected_data (pd.DataFrame, optional): 收录曲目数据
        ranking_type (str): 榜单类型(daily/weekly/monthly/annual/special)
        old_time_toll (str): 旧数据时间阈值（格式：YYYYMMDD）
    
    返回:
        pd.DataFrame: 处理后的结果
    """
    result = []
    iterator = new_data.iterrows()
    
    for _, record in iterator:
        # 获取BV号
        bvid = record.get('bvid')
        if not bvid:
            continue

        # 获取新数据记录
        new_match = new_data['bvid'] == bvid
        if not new_match.any():
            continue
        new = new_data[new_match].squeeze()
        
        # 处理旧数据匹配
        old = None
        if use_old_data:
            old_match = old_data['bvid'] == bvid
            if old_match.any(): old = old_data[old_match].squeeze()
            else:
                # 检查发布时间是否在限定范围内
                pubdate = datetime.strptime(new['pubdate'], "%Y-%m-%d %H:%M:%S")
                threshold = datetime.strptime(old_time_toll, "%Y%m%d")
                if pubdate < threshold: continue
                # 旧视频缺省值
                old = {'view': 0, 'favorite': 0, 'coin': 0, 'like': 0}
        
        # 需要通过收录曲目信息补充
        if collected_data is not None:
            coll_match = collected_data['bvid'] == bvid
            if coll_match.any():
                coll_rec = collected_data[coll_match].squeeze()
                # 待补充的字段
                for field in ['name', 'author', 'synthesizer', 'copyright', 'vocal', 'type']:
                    new[field] = coll_rec.get(field, new[field])
        
        # 计算各项数据
        data = calculate(new, old, ranking_type)
        result.append({
            'title': new['title'], 'bvid': bvid, 'name': new['name'], 
            'author': new['author'], 'uploader': new['uploader'], 
            'copyright': new['copyright'], 'synthesizer': new['synthesizer'], 
            'vocal': new['vocal'], 'type': new['type'], 'pubdate': new['pubdate'], 
            'duration': new['duration'], 'page': new['page'], 
            'view': data[0], 'favorite': data[1], 'coin': data[2], 
            'like': data[3], 
            'viewR': f'{data[4]:.2f}', 'favoriteR': f'{data[5]:.2f}', 
            'coinR': f'{data[6]:.2f}', 'likeR': f'{data[7]:.2f}',
            'fixA': f'{data[8]:.2f}', 'fixB': f'{data[9]:.2f}', 
            'fixC': f'{data[10]:.2f}', 'point': data[11], 
            'image_url': new['image_url']
        })
        # 添加额外信息(如果需要)
        if use_collected:
            result[-1].update({'tags': new['tags'], 'description': new['description']})
    
    return pd.DataFrame(result)