# utils/data_handler.py
import pandas as pd
from pathlib import Path
import json
from utils.config_handler import ConfigHandler
from utils.io_utils import save_to_excel

class DataHandler:
    def __init__(self, config_handler: ConfigHandler):
        self.config = config_handler
        with open('config/usecols.json', 'r', encoding='utf-8') as f:
            usecols_data = json.load(f)
            self.usecols = usecols_data.get('columns', {})
            self.maps = usecols_data.get('maps', {})
            
    def _read_excel(self, path: Path, usecols_key: str = 'stat') -> pd.DataFrame:
        if path.exists():
            return pd.read_excel(path, usecols=self.usecols.get(usecols_key))
        return pd.DataFrame()

    def load_merged_data(self, date: str) -> pd.DataFrame:
        """
        合并指定日期的主数据(toll)和新曲数据(new)。
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