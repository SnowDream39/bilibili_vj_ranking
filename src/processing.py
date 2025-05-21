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
    通用数据处理函数
    
    参数:
        new_data (pd.DataFrame): 新数据集
        records (pd.DataFrame, optional): 需要处理的记录集（若为None则处理new_data所有记录）
        old_data (pd.DataFrame, optional): 旧数据集
        use_old_data (bool): 是否使用旧数据对比
        use_collected (bool): 是否是部分打标的新曲
        collected_data (pd.DataFrame, optional): 收录曲目数据
        ranking_type (str): 排行榜类型参数
        old_time_toll (str): 旧数据时间阈值（格式：YYYYMMDD）
    
    返回:
        pd.DataFrame: 处理后的结果
    """
    result = []
    iterator = new_data.iterrows()
    
    for _, record in iterator:
        bvid = record.get('bvid')
        if not bvid:
            continue
        
        new_match = new_data['bvid'] == bvid
        if not new_match.any():
            continue
        new = new_data[new_match].squeeze()
        
        old = None
        if use_old_data:
            
            old_match = old_data['bvid'] == bvid
            if old_match.any(): old = old_data[old_match].squeeze()
            else:
                pubdate = datetime.strptime(new['pubdate'], "%Y-%m-%d %H:%M:%S")
                threshold = datetime.strptime(old_time_toll, "%Y%m%d")
                if pubdate < threshold: continue
                old = {'view': 0, 'favorite': 0, 'coin': 0, 'like': 0}
        
        if use_collected and collected_data is not None:
            coll_match = collected_data['bvid'] == bvid
            if coll_match.any():
                coll_rec = collected_data[coll_match].squeeze()
                for field in ['name', 'author', 'synthesizer', 'copyright', 'vocal', 'type']:
                    new[field] = coll_rec.get(field, new[field])
        
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
        if use_collected:
            result[-1].update({'tags': new['tags'], 'description': new['description']})
    
    return pd.DataFrame(result)