# utils/calculator.py
# 计算模块: 处理视频数据的计算逻辑
# 包括播放、收藏、硬币、点赞等数据的分数计算和排名更新

import pandas as pd
from math import ceil, floor
from utils.io_utils import format_columns

def calculate_scores(view: int, favorite: int, coin: int, like: int, copyright: int, ranking_type: str):
    """
    计算视频的各项评分
    
    Args:
        view: 播放
        favorite: 收藏
        coin: 硬币
        like: 点赞
        copyright: 版权类型(1,3为自制,2为转载)
        ranking_type: 榜单类型(daily/weekly/monthly/annual/special)
    
    Returns:
        tuple: (播放分,收藏分,硬币分,点赞分,修正系数A,修正系数B,修正系数C)
    """
    # 版权判定: 自制=1, 转载=2
    copyright = 1 if copyright in [1, 3] else 2
    # 特殊情况处理: 如果有其他互动但没有投币,设为1
    coin = 1 if (coin == 0 and view > 0 and favorite > 0 and like > 0) else coin  
    # 计算修正系数A(针对搬运硬币补偿)
    fixA = 0 if coin <= 0 else (1 if copyright == 1 else ceil(max(1, (view + 20 * favorite + 40 * coin + 10 * like) / (200 * coin)) * 100) / 100)  
    
    # 计算修正系数B(云视听小电视抑制系数)
    if ranking_type in ('daily', 'weekly', 'monthly'):
        fixB = 0 if view + 20 * favorite <= 0 else ceil(min(1, 3 * max(0, (20 * coin + 10 * like)) / (view + 20 * favorite)) * 100) / 100
    elif ranking_type in ('annual', 'special'):
        fixB = 0 if view + 20 * favorite <= 0 else ceil(min(1, 3 * max(0, (20 * coin * fixA + 10 * like)) / (view + 20 * favorite)) * 100) / 100

    # 计算修正系数C(梗曲抑制系数)
    fixC = 0 if like + favorite <= 0 else ceil(min(1, (like + favorite + 20 * coin * fixA)/(2 * like + 2 * favorite)) * 100) / 100

    # 日刊/周刊评分计算
    if ranking_type in ('daily', 'weekly'):
        viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 10 / view, 1) * 100) / 100, 0)
        favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 10 + view) * 20, 20) * 100) / 100, 0)
        coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 20 + view) * 40, 40) * 100) / 100, 0)
        likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)
    # 月刊/年刊/特刊评分计算
    elif ranking_type in ('monthly', 'annual', 'special'):
        viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 15 / view, 1) * 100) / 100, 0)
        favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 10 + view) * 20, 20) * 100) / 100, 0)
        coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 20 + view) * 40, 40) * 100) / 100, 0)
        likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)

    return viewR, favoriteR, coinR, likeR, fixA, fixB, fixC

def calculate_points(diff, scores):
    """
    计算总分
    
    Args:
        diff: 各项数据的增量
        scores: calculate_scores返回的评分
    
    Returns:
        float: 计算得到的总分
    """
    # 处理特殊情况: 如果没有投币但有其他互动, 则将硬币虚设为1
    coin =  1 if (diff[2] == 0 and diff[0] > 0 and diff[1] > 0 and diff[3] > 0) else diff[2]
    
    # 计算各项分数
    viewR, favoriteR, coinR, likeR, fixA = scores[:5]
    viewP = diff[0] * viewR             # 播放得分
    favoriteP = diff[1] * favoriteR     # 收藏得分
    coinP = coin * coinR * fixA         # 硬币得分
    likeP = diff[3] * likeR             # 点赞得分
    return viewP + favoriteP + coinP + likeP

def calculate_ranks(df):
    """
    计算视频排名
    为播放、收藏、硬币、点赞和总分分别计算排名
    """
    df = df.sort_values('point', ascending=False)
    for col in ['view', 'favorite', 'coin', 'like']:
        df[f'{col}_rank'] = df[col].rank(ascending=False, method='min')
    df['rank'] = df['point'].rank(ascending=False, method='min')
    return format_columns(df)

def update_rank_and_rate(df_today, prev_file_path):
    """
    更新排名和增长率
    
    Args:
        df_today: 当前数据
        prev_file_path: 前一期数据文件路径
    """
    df_prev = pd.read_excel(prev_file_path)
    prev_dict = df_prev.set_index('name')[['rank', 'point']].to_dict(orient='index')

    # 添加上期排名和分数
    df_today['rank_before'] = df_today['name'].map(lambda x: prev_dict.get(x, {}).get('rank', '-'))
    df_today['point_before'] = df_today['name'].map(lambda x: prev_dict.get(x, {}).get('point', '-'))

    # 计算增长率 
    # rate = (当前分数 - 上期分数) / 上期分数
    df_today['rate'] = df_today.apply(
        lambda row: (
            'NEW' if row['point_before'] == '-' else
            'inf' if row['point_before'] == 0 else
            f"{(row['point'] - row['point_before']) / row['point_before']:.2%}"
        ), axis=1
    )
    df_today = df_today.sort_values('point', ascending=False)
    return df_today

def update_count(df_today, prev_file_path):
    """更新视频上榜次数"""
    df_prev = pd.read_excel(prev_file_path)
    prev_count_dict = df_prev.set_index('name')['count'].to_dict()
    # 如果当前排名≤20则上榜次数+1
    df_today['count'] = df_today['name'].map(lambda x: prev_count_dict.get(x, 0)) + (df_today['rank'] <= 20).astype(int)
    return df_today

def calculate_differences(new: pd.DataFrame, ranking_type: str, old = None):
    """
    计算数据差值
    
    Args:
        new: 新数据
        ranking_type: 榜单类型
        old: 旧数据(可选，否则置为0)
    
    Returns:
        dict: 包含各项数据差值的字典
    """
    if ranking_type in ('daily', 'weekly', 'monthly', 'annual'):
        return {col: new[col] - old.get(col, 0) for col in ['view', 'favorite', 'coin', 'like']}
    # 特刊按总数据值计算
    elif ranking_type == 'special':
        return {col: new[col] for col in ['view', 'favorite', 'coin', 'like']}
    
def calculate(new: pd.DataFrame, old: pd.DataFrame, ranking_type: str):
    """
    计算完整评分
    包括差值计算、评分计算和总分计算
    """
    diff = [calculate_differences(new, ranking_type, old)[col] for col in ['view', 'favorite', 'coin', 'like']]
    scores = calculate_scores(*diff, new['copyright'], ranking_type)
    point = round(scores[5] * scores[6] * calculate_points(diff, scores))
    
    return diff + list(scores) + [point]

def merge_duplicate_names(df):
    """
    合并具有相同曲名的重复记录
    保留得分最高的一个记录
    """
    merged_df = pd.DataFrame()
    grouped = df.groupby('name')
      
    for _, group in grouped:
        if len(group) > 1:
            # 获取组内最高分的记录
            best_record = group.loc[group['point'].idxmax()].copy() 
            # 将最高分记录添加到结果中
            merged_df = pd.concat([merged_df, best_record.to_frame().T])
        else: 
            # 无重复则直接添加
            merged_df = pd.concat([merged_df, group])
    return merged_df