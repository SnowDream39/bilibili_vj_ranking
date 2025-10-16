# utils/processing.py
# 数据处理模块：视频数据的清洗、合并和评分计算
from typing import Optional
import pandas as pd
from datetime import datetime
from utils.calculator import calculate

def process_records(
    new_data: pd.DataFrame,
    old_data: Optional[pd.DataFrame] = None,
    use_old_data: bool = False,
    collected_data: Optional[pd.DataFrame] = None,
    ranking_type: str = 'daily',
    old_time_toll: Optional[str] = None
) -> pd.DataFrame:
    """处理一批视频记录，根据新旧数据计算增量得分，并可选择性地合并收录信息。

    该函数是数据处理的核心，它遍历新数据中的每条记录，根据配置匹配旧数据
    和收录数据，调用计算模块生成分数，最终整合成一个DataFrame。

    Args:
        new_data (pd.DataFrame): 新获取的视频数据。
        old_data (pd.DataFrame, optional): 上期数据，用于计算增量。
        use_old_data (bool): 是否使用上期数据进行对比。
        collected_data (pd.DataFrame, optional): 包含完整元数据的收录曲目列表。
        ranking_type (str): 榜单类型（'daily', 'weekly', etc.）。
        old_time_toll (str, optional): 旧数据时间阈值（格式：YYYYMMDD），用于过滤新曲。

    Returns:
        pd.DataFrame: 包含计算结果和完整信息的处理后数据。
    """
    result = []
    iterator = new_data.iterrows()
    
    for _, record in iterator:
        # 获取当前记录的bvid作为唯一标识
        bvid = record.get('bvid')
        if not bvid:
            continue

        # 在新数据中定位当前bvid的完整记录
        new_match = new_data['bvid'] == bvid
        if not new_match.any():
            continue
        new = new_data[new_match].squeeze()
        
        # 如果需要，匹配并处理旧数据
        old: Optional[pd.Series] = None
        if use_old_data and old_data is not None :
            # 在旧数据中查找匹配的记录
            old_match = old_data['bvid'] == bvid
            if old_match.any(): old = old_data[old_match].squeeze()
            elif old_time_toll is not None:
                # 对于旧数据中没有的视频，检查其发布时间
                pubdate = datetime.strptime(new['pubdate'], "%Y-%m-%d %H:%M:%S")
                threshold = datetime.strptime(old_time_toll, "%Y%m%d")
                # 如果发布时间早于统计周期，则跳过 (属于更早的视频)
                if pubdate < threshold: continue
                # 周期内的新视频，则创建一个全为0的旧数据记录用于计算增量
                old = pd.Series({'view': 0, 'favorite': 0, 'coin': 0, 'like': 0})
        
        # 需要通过收录曲目信息补充
        if collected_data is not None:
            coll_match = collected_data['bvid'] == bvid
            if coll_match.any():
                coll_rec = collected_data[coll_match].squeeze()
                # 遍历需要补充的字段，用收录列表中的信息进行更新
                for field in ['name', 'author', 'synthesizer', 'copyright', 'vocal', 'type']:
                    new[field] = coll_rec.get(field, new[field])
        
        # 调用计算模块获取得分和各项系数
        data = calculate(new, old, ranking_type)
        
        record_dict = {
            'title': new['title'], 'bvid': bvid, 'aid': new['aid'], 'name': new['name'], 
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
        }
        if 'intro' in new and pd.notna(new['intro']):
            record_dict['intro'] = new['intro']

        result.append(record_dict)
    return pd.DataFrame(result)