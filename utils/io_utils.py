# utils/io_utils.py
import pandas as pd
from pathlib import Path
from typing import Optional, List, Union
from openpyxl.utils import get_column_letter
from utils.logger import logger

def save_to_excel(df: pd.DataFrame, filename: Union[str, Path], usecols: Optional[List[str]] = None):
    if usecols:
        cols_to_save = [col for col in usecols if col in df.columns]
        df = df[cols_to_save]
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']
            
            if 'pubdate' in df.columns:
                pubdate_col = get_column_letter(df.columns.get_loc('pubdate') + 1)
                for cell in worksheet[pubdate_col]:
                    cell.number_format = '@'
                    cell.alignment = cell.alignment.copy(horizontal='left')
                
        logger.info(f"{filename} 保存完成")
    except Exception as e:
        logger.warning(f"Excel 保存失败：{e}")

        # 备份 CSV
        backup_csv = Path(filename).with_suffix('.csv')
        df.to_csv(backup_csv, index=False, encoding='utf-8-sig')
        logger.info(f"数据已备份至 {backup_csv}")

def format_columns(df):
    columns = ['viewR', 'favoriteR', 'coinR', 'likeR', 'fixA', 'fixB', 'fixC']
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: f'{x:.2f}' if pd.notnull(x) else '')
    return df