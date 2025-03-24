import pandas as pd
from math import ceil, floor

def calculate_scores(view: int, favorite: int, coin: int, like: int, copyright: int, ranking_type: str):
    copyright = 1 if copyright in [1, 3] else 2
    coin = 1 if (coin == 0 and view > 0 and favorite > 0 and like > 0) else coin  
    fixA = 0 if coin <= 0 else (1 if copyright == 1 else ceil(max(1, (view + 20 * favorite + 40 * coin + 10 * like) / (200 * coin)) * 100) / 100)  
    
    if ranking_type in ('daily', 'weekly', 'monthly'):
        fixB = 0 if view + 20 * favorite <= 0 else ceil(min(1, 3 * max(0, (20 * coin + 10 * like)) / (view + 20 * favorite)) * 100) / 100
    elif ranking_type == 'special':
        fixB = 0 if view + 20 * favorite <= 0 else ceil(min(1, 3 * max(0, (20 * coin * fixA + 10 * like)) / (view + 20 * favorite)) * 100) / 100

    fixC = 0 if like + favorite <= 0 else ceil(min(1, (like + favorite + 20 * coin * fixA)/(2 * like + 2 * favorite)) * 100) / 100

    if ranking_type in ('daily', 'weekly'):
        viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 20 / view, 1) * 100) / 100, 0)
        favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 20 + view) * 40, 20) * 100) / 100, 0)
        coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 40 + view) * 80, 40) * 100) / 100, 0)
        likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)
    elif ranking_type in ('monthly', 'special'):
        viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 25 / view, 1) * 100) / 100, 0)
        favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 15 + view) * 40, 20) * 100) / 100, 0)
        coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 30 + view) * 80, 40) * 100) / 100, 0)
        likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)

    return viewR, favoriteR, coinR, likeR, fixA, fixB, fixC

def calculate_points(diff, scores):
    diff[2] =  1 if (diff[2] == 0 and diff[0] > 0 and diff[1] > 0 and diff[3] > 0) else diff[2]
    viewR, favoriteR, coinR, likeR, fixA = scores[:5]
    viewP = diff[0] * viewR
    favoriteP = diff[1] * favoriteR
    coinP = diff[2] * coinR * fixA
    likeP = diff[3] * likeR
    return viewP + favoriteP + coinP + likeP

def calculate_ranks(df):
    df = df.sort_values('point', ascending=False)
    for col in ['view', 'favorite', 'coin', 'like']:
        df[f'{col}_rank'] = df[col].rank(ascending=False, method='min')
    df['rank'] = df['point'].rank(ascending=False, method='min')
    return df

def update_rank_and_rate(df_today, prev_file_path):
    df_prev = pd.read_excel(prev_file_path)
    prev_dict = df_prev.set_index('name')[['rank', 'point']].to_dict(orient='index')

    df_today['rank_before'] = df_today['name'].map(lambda x: prev_dict.get(x, {}).get('rank', '-'))
    df_today['point_before'] = df_today['name'].map(lambda x: prev_dict.get(x, {}).get('point', '-'))
    
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
    df_prev = pd.read_excel(prev_file_path)
    prev_count_dict = df_prev.set_index('name')['count'].to_dict()
    df_today['count'] = df_today['name'].map(lambda x: prev_count_dict.get(x, 0)) + (df_today['rank'] <= 20).astype(int)
    return df_today

def calculate_differences(new: pd.DataFrame, ranking_type: str, old = None):
    if ranking_type in ('daily', 'weekly', 'monthly'):
        return {col: new[col] - old.get(col, 0) for col in ['view', 'favorite', 'coin', 'like']}
    elif ranking_type == 'special':
        return {col: new[col] for col in ['view', 'favorite', 'coin', 'like']}
    
def calculate(new: pd.DataFrame, old: pd.DataFrame, ranking_type: str):
    diff = [calculate_differences(new, ranking_type, old)[col] for col in ['view', 'favorite', 'coin', 'like']]
    scores = calculate_scores(*diff, new['copyright'], ranking_type)
    point = round(scores[5] * scores[6] * calculate_points(diff, scores))
    
    return diff + list(scores) + [point]

