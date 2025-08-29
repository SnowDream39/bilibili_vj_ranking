# utils/data_handler.py
# 数据处理器模块：管理数据的读取、合并和保存操作
import pandas as pd
from pathlib import Path
from typing import Optional
import json
from utils.config_handler import ConfigHandler
from utils.io_utils import save_to_excel

class DataHandler:
    """
    数据处理器类
    负责所有数据文件的读取、合并和保存操作
    """
    def __init__(self, config_handler: ConfigHandler):
        """
        初始化数据处理器，加载列配置。
        
        Args:
            config_handler (ConfigHandler): 配置处理器实例，用于获取路径等配置。
        """
        self.config = config_handler
        # 从JSON文件加载列配置和映射关系
        with open('config/usecols.json', 'r', encoding='utf-8') as f:
            usecols_data = json.load(f)
            self.usecols = usecols_data.get('columns', {})
            self.maps = usecols_data.get('maps', {})
            
    def _read_excel(self, path: Path, usecols_key: str = 'stat') -> pd.DataFrame:
        """读取指定的Excel文件，如果文件不存在则返回空DataFrame。

        Args:
            path (Path): Excel文件的路径。
            usecols_key (str): 用于从配置中获取待读取列的键名，默认为'stat'。

        Returns:
            pd.DataFrame: 读取的数据。
        """
        if path.exists():
            # 如果文件存在，则使用指定的列配置读取
            return pd.read_excel(path, usecols=self.usecols.get(usecols_key))
        # 如果文件不存在，返回一个空的DataFrame以避免错误
        return pd.DataFrame()

    def load_merged_data(self, date: str) -> pd.DataFrame:
        """加载并合并指定日期的主数据（旧曲）和新曲数据。

        Args:
            date (str): 用于定位数据文件的日期字符串 (YYYYMMDD)。

        Returns:
            pd.DataFrame: 合并后的数据集。
        """
        # 获取旧曲和新曲数据的文件路径
        toll_path = self.config.get_data_source_path('toll_data', date=date)
        new_path = self.config.get_data_source_path('new_data', date=date)
        # 分别读取两个数据文件
        toll_data = self._read_excel(toll_path, usecols_key='stat')
        new_data = self._read_excel(new_path, usecols_key='stat')

        if not new_data.empty:
            # 如果新曲数据不为空，则进行合并
            # 使用concat合并，并根据bvid去重，保留首次出现的记录
            return pd.concat([toll_data, new_data]).drop_duplicates(subset=['bvid'], keep='first')
        return toll_data

    def load_toll_data(self, date: str) -> pd.DataFrame:
        """加载指定日期的主数据（旧曲）。

        Args:
            date (str): 日期字符串 (YYYYMMDD)。

        Returns:
            pd.DataFrame: 主数据（旧曲）的DataFrame。
        """
        toll_path = self.config.get_data_source_path('toll_data', date=date)
        return self._read_excel(toll_path, usecols_key='stat')

    def save_df(self, df: pd.DataFrame, path: Path, usecols_key: Optional[str] = None):
        """将DataFrame保存到指定的路径，可选择性地只保存特定列。

        Args:
            df (pd.DataFrame): 待保存的DataFrame。
            path (Path): 保存的目标文件路径。
            usecols_key (str, optional): 用于从配置中获取待保存列的键名。
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        cols_to_use = self.usecols.get(usecols_key) if usecols_key else None
        save_to_excel(df, path, usecols=cols_to_use)