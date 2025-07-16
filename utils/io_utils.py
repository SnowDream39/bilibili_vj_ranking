# utils/io_utils.py
# IO工具模块: 处理文件读写相关操作
# 包括Excel保存和数据格式化
import pandas as pd
from pathlib import Path
from typing import Optional, List, Union
from openpyxl.utils import get_column_letter
from utils.logger import logger

def save_to_excel(df: pd.DataFrame, filename: Union[str, Path], usecols: Optional[List[str]] = None):
    """
    保存DataFrame到Excel文件
       
    Args:
        df: 要保存的DataFrame
        filename: 保存路径(字符串或Path对象)
        usecols: 可选,指定要保存的列名列表
    """
    if usecols:
        cols_to_save = [col for col in usecols if col in df.columns]
        df = df[cols_to_save].copy() 
    try:
        # aid列数据处理:转整数格式
        if 'aid' in df.columns:
            df['aid'] = df['aid'].apply(lambda x: "{:.0f}".format(float(x)) if pd.notna(x) and str(x).strip() != '' else '')

        # 写入Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']
            
            # pubdate列格式设置:文本格式+左对齐
            if 'pubdate' in df.columns:
                pubdate_col = get_column_letter(df.columns.get_loc('pubdate') + 1)
                for cell in worksheet[pubdate_col]:
                    cell.number_format = '@'
                    cell.alignment = cell.alignment.copy(horizontal='left')
            
            # aid列格式设置:文本格式+左对齐
            if 'aid' in df.columns:
                aid_col_letter = get_column_letter(df.columns.get_loc('aid') + 1)
                for cell in worksheet[aid_col_letter]:
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
    """
    格式化DataFrame中的特定列

    Args:
        df: 要处理的DataFrame
        
    Returns:
        DataFrame: 处理后的DataFrame
    """
    columns = ['viewR', 'favoriteR', 'coinR', 'likeR', 'fixA', 'fixB', 'fixC']
    for col in columns:
        if col in df.columns:
            # 转换为数值类型
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # 格式化数值:保留2位小数
            df[col] = df[col].apply(lambda x: f'{x:.2f}' if pd.notnull(x) else '')
    return df