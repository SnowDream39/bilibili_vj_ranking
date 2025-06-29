# utils/config_handler.py
from pathlib import Path
import yaml
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class ConfigHandler:
    def __init__(self, period: str):
        with open('config/rankings.yaml', 'r', encoding='utf-8') as f:
            all_configs = yaml.safe_load(f)
        self.period = period 
        self.config = all_configs[period]
        self.data_sources = all_configs.get('data_sources', {})

    def get_path(self, key: str, path_type: str = 'output_paths', **kwargs) -> Path:
        """
        根据key和日期参数动态生成完整路径。
        """
        template = self.config[path_type][key]
        path = Path(template.format(**kwargs))
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_data_source_path(self, key: str, date: str) -> Path:
        template = self.data_sources[key]
        return Path(template.format(date=date))

    @staticmethod
    def get_weekly_dates():
        today = datetime.now()
        new_day = today - timedelta(days=(today.weekday() - 5 + 7) % 7)
        old_day = new_day - timedelta(days=7)
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d'),
            "target_date": new_day.strftime('%Y-%m-%d'),
            "previous_date": old_day.strftime('%Y-%m-%d')
        }

    @staticmethod
    def get_monthly_dates():
        new_day = datetime.now().replace(day=1)
        new_month = new_day - timedelta(days=1)
        old_day = new_month.replace(day=1)
        old_month = old_day - relativedelta(months=1)
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d'),
            "target_date": new_month.strftime('%Y-%m'),
            "previous_date": old_month.strftime('%Y-%m')
        }
    
    @staticmethod
    def get_daily_dates():
        now_day = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        new_day = now_day + timedelta(days=1)
        old_day = now_day
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d')
        }
    
    @staticmethod
    def get_daily_new_song_dates():
        now_day = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        new_day = now_day + timedelta(days=1)
        old_day = now_day - timedelta(days=1)
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "now_date": now_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d')
        }