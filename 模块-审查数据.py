import numpy as np
import pandas as pd
from pathlib import Path
from itertools import combinations
from collections import defaultdict
import requests
import imagehash
from PIL import Image
import io
import hashlib
from tqdm import tqdm
from utils.io_utils import save_to_excel
from utils.logger import logger

INPUT_FILE = Path("收录曲目.xlsx")
OUTPUT_FILE = Path("疑似重复曲目.xlsx")
IMAGE_CACHE_DIR = Path("image_cache") 

# 感知哈希相似度阈值。
HASH_SIMILARITY_THRESHOLD = 15
# 日期差距阈值 (天)
DATE_DIFFERENCE_DAYS = 3

def normalize_multi_value_field(value: str, separator: str = '、') -> str:
    if not isinstance(value, str):
        return str(value)
    parts = [part.strip() for part in value.split(separator)]
    sorted_parts = sorted([p for p in parts if p])
    return ",".join(sorted_parts)

def get_image_hash(url: str, cache_dir: Path, hash_cache: dict) -> imagehash.ImageHash | None:
    if not isinstance(url, str) or not url.startswith('http'):
        return None

    if url in hash_cache:
        return hash_cache[url]

    url_hash = hashlib.sha256(url.encode()).hexdigest()
    cache_path = cache_dir / f"{url_hash}.jpg"

    try:
        if cache_path.exists():
            with Image.open(cache_path) as img:
                h = imagehash.phash(img)
        else:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            with open(cache_path, 'wb') as f:
                f.write(response.content)
            with Image.open(io.BytesIO(response.content)) as img:
                h = imagehash.phash(img)
        
        hash_cache[url] = h
        return h
    except (requests.exceptions.RequestException, IOError, Exception):
        hash_cache[url] = None
        return None


def find_name_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("查找相同曲名")
    key_info_columns = ['author', 'synthesizer', 'vocal', 'type', 'copyright']
    multi_value_cols = ['author', 'synthesizer', 'vocal']
    other_key_cols = ['author', 'synthesizer', 'vocal', 'type']

    duplicates = df[df.duplicated(subset=['name'], keep=False)].copy()
    if duplicates.empty:
        return pd.DataFrame()
    
    controversial_indices = []

    for _, group in duplicates.groupby('name'):
        
        group_to_compare = group[key_info_columns].copy()
        for col in multi_value_cols:
            group_to_compare[col] = group_to_compare[col].astype(str).apply(normalize_multi_value_field)
        group_to_compare['type'] = group_to_compare['type'].astype(str).str.strip()
        group_to_compare['copyright'] = pd.to_numeric(
            group_to_compare['copyright'], errors='coerce'
        ).fillna(0).astype(int)

        copyright_values = set(group_to_compare['copyright'].unique())
        is_copyright_controversial = (1 in copyright_values and 
                                      bool(copyright_values.intersection({2, 3, 4})))
        other_rows_as_tuples = [tuple(rec) for rec in group_to_compare[other_key_cols].to_records(index=False)]
        is_other_fields_controversial = len(set(other_rows_as_tuples)) > 1

        if is_copyright_controversial or is_other_fields_controversial:
            controversial_indices.extend(group.index.tolist())
    
    if not controversial_indices:
        return pd.DataFrame()
    final_duplicates = df.loc[list(set(controversial_indices))].copy()
    sort_columns = ['name'] + key_info_columns
    sorted_duplicates = final_duplicates.sort_values(by=sort_columns)
    return sorted_duplicates


def find_similar_images_by_author(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("通过作者、不同UP主和相似图片查找")
    IMAGE_CACHE_DIR.mkdir(exist_ok=True)
    
    found_indices = set()
    hash_cache = {}
    
    grouped = df.groupby('author')
    for _, group in tqdm(grouped, desc="分析作者分组 (图片)"):
        if len(group) < 2:
            continue
        
        for i, j in combinations(group.index, 2):
            row1 = group.loc[i]
            row2 = group.loc[j]
            
            if row1['name'] == row2['name'] or row1['uploader'] == row2['uploader']:
                continue
            
            hash1 = get_image_hash(row1['image_url'], IMAGE_CACHE_DIR, hash_cache)
            hash2 = get_image_hash(row2['image_url'], IMAGE_CACHE_DIR, hash_cache)
            
            if hash1 and hash2:
                distance = hash1 - hash2
                if distance <= HASH_SIMILARITY_THRESHOLD:
                    found_indices.add(i)
                    found_indices.add(j)
    
    if not found_indices:
        return pd.DataFrame()
        
    result_df = df.loc[list(found_indices)].sort_values(by=['author', 'name'])
    return result_df


def find_close_pubdates_by_author(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("通过作者、不同UP主和相近发布日期查找")
    found_indices = set()
    df['pubdate_dt'] = pd.to_datetime(df['pubdate'], errors='coerce')
    
    grouped = df.groupby('author')
    for _, group in tqdm(grouped, desc="分析作者分组 (日期)"):
        if len(group) < 2:
            continue
            
        for i, j in combinations(group.index, 2):
            row1 = group.loc[i]
            row2 = group.loc[j]
            
            if (row1['name'] == row2['name'] or 
                row1['uploader'] == row2['uploader'] or 
                pd.isna(row1['pubdate_dt']) or 
                pd.isna(row2['pubdate_dt'])):
                continue
            
            time_diff = abs(row1['pubdate_dt'] - row2['pubdate_dt'])
            
            if time_diff <= pd.Timedelta(days=DATE_DIFFERENCE_DAYS):
                found_indices.add(i)
                found_indices.add(j)
    
    df.drop(columns=['pubdate_dt'], inplace=True, errors='ignore')
    
    if not found_indices:
        return pd.DataFrame()

    result_df = df.loc[list(found_indices)].sort_values(by=['author', 'pubdate'])
    return result_df


def main():
    if not INPUT_FILE.exists():
        logger.info(f"错误: 输入文件 '{INPUT_FILE}' 不存在。")
        return

    try:
        df = pd.read_excel(INPUT_FILE, dtype={'aid': str})
    except Exception as e:
        logger.info(f"读取 Excel 文件时出错: {e}")
        return

    for col in ['name', 'author', 'image_url', 'uploader']:
        if col not in df.columns:
            raise
        df[col] = df[col].astype(str).fillna('')

    df_name_dupes = find_name_duplicates(df)
    df_image_sim = find_similar_images_by_author(df)
    df_date_close = find_close_pubdates_by_author(df)

    reasons = defaultdict(list)
    for idx in df_name_dupes.index:
        reasons[idx].append("相同曲名")
    for idx in df_image_sim.index:
        reasons[idx].append("图片相似")
    for idx in df_date_close.index:
        reasons[idx].append("日期相近")

    all_found_indices = set(reasons.keys())
    if not all_found_indices:
        logger.info("未找到任何疑似重复的记录。")
        return

    final_df = df.loc[list(all_found_indices)].copy()
    final_df['reason'] = final_df.index.map(lambda idx: "；".join(sorted(list(set(reasons[idx])))))

    final_df = final_df.sort_values(by=['author', 'name', 'pubdate'])
    
    cols = final_df.columns.tolist()
    if 'reason' in cols:
        cols.pop(cols.index('reason'))
        cols.append('reason')
        final_df = final_df[cols]

    logger.info(f"共找到 {len(final_df)} 条疑似重复记录")
    
    save_to_excel(final_df, OUTPUT_FILE)


if __name__ == "__main__":
    main()
