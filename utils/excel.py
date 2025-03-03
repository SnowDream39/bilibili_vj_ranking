import pandas as pd
import re
from pathlib import Path
from typing import Optional, List
from openpyxl.utils import get_column_letter

def remove_illegal_chars(text):
    # 移除非法的控制字符，保留 \t, \n, \r
    if isinstance(text, str):
        return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)
    return text

def output_excel(df: pd.DataFrame, filename: str | Path, usecols: Optional[List[str]] = None):
    if usecols:
        df = df[usecols]
    for column in df.columns:
        df[column] = df[column].apply(lambda x: remove_illegal_chars(x))
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']
            
            pubdate_col = get_column_letter(df.columns.get_loc('pubdate') + 1)
            for cell in worksheet[pubdate_col]:
                cell.number_format = '@'
                cell.alignment = cell.alignment.copy(horizontal='left')
                
        print(f"{filename} 保存完成")
    except Exception as e:
        print(f"Excel 保存失败：{e}")

        # 备份 CSV
        backup_csv = Path(filename).with_suffix('.csv')
        df.to_csv(backup_csv, index=False, encoding='utf-8-sig')
        print(f"数据已备份至 {backup_csv}")