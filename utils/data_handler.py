# utils/data_handler.py
# 数据处理器模块：管理数据的读取、合并和保存操作
import pandas as pd
from pathlib import Path
import json
from utils.config_handler import ConfigHandler
from utils.io_utils import save_to_excel

class DataHandler:
    """
    数据处理器类
    负责所有数据文件的读取、合并和保存操作
    
    主要功能:
    1. 读取配置的列字段
    2. 加载Excel数据
    3. 合并旧曲和新曲数据
    4. 保存处理后的数据
    """
    def __init__(self, config_handler: ConfigHandler):
        """
        初始化数据处理器
        
        Args:
            config_handler: 配置处理器实例
                          用于获取数据源路径等配置信息
        """
        self.config = config_handler
        # 读取列配置
        with open('config/usecols.json', 'r', encoding='utf-8') as f:
            usecols_data = json.load(f)
            self.usecols = usecols_data.get('columns', {})
            self.maps = usecols_data.get('maps', {})
            
    def _read_excel(self, path: Path, usecols_key: str = 'stat') -> pd.DataFrame:
        """
        读取Excel文件的辅助函数
        
        Args:
            path: Excel文件路径
            usecols_key: 列配置的键名,默认'stat'
                        用于从usecols配置中获取要读取的列
        
        Returns:
            DataFrame: 读取的数据,如果文件不存在返回空DataFrame
        """
        if path.exists():
            return pd.read_excel(path, usecols=self.usecols.get(usecols_key))
        return pd.DataFrame()

    def load_merged_data(self, date: str) -> pd.DataFrame:
        """
        加载并合并指定日期的主数据和新曲数据
        
        处理流程:
        1. 获取旧曲数据和新曲数据的文件路径
        2. 分别读取两个数据文件
        3. 如果有新曲数据,进行合并
           - 使用concat横向合并
           - 根据bvid去重,保留首次出现的记录
        
        Args:
            date: 日期字符串,用于定位数据文件
            
        Returns:
            DataFrame: 合并后的数据集
        """
        toll_path = self.config.get_data_source_path('toll_data', date=date)
        new_path = self.config.get_data_source_path('new_data', date=date)

        toll_data = self._read_excel(toll_path, usecols_key='stat')
        new_data = self._read_excel(new_path, usecols_key='stat')

        if not new_data.empty:
            return pd.concat([toll_data, new_data]).drop_duplicates(subset=['bvid'], keep='first')
        return toll_data

    def load_toll_data(self, date: str) -> pd.DataFrame:
        """
        加载指定日期的主数据(toll)。

        Args:
            date: 日期字符串
            
        Returns:
            DataFrame: 主榜数据
        """
        toll_path = self.config.get_data_source_path('toll_data', date=date)
        return self._read_excel(toll_path, usecols_key='stat')

    def save_df(self, df: pd.DataFrame, path: Path, usecols_key: str = None):
        """
        保存 DataFrame。
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        cols_to_use = self.usecols.get(usecols_key) if usecols_key else None
        save_to_excel(df, path, usecols=cols_to_use)