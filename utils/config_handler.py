# utils/config_handler.py
# 配置处理器模块：管理项目配置和日期处理
from pathlib import Path
from typing import Optional
import yaml
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class ConfigHandler:
    """
    处理项目配置，负责加载YAML文件、生成动态路径和计算不同周期的日期。
    """
    def __init__(self, period: str):
        """初始化配置处理器。

            period (str): 周期类型，如 'daily', 'weekly', 'monthly'。
        """
        # 读取配置文件
        with open('config/rankings.yaml', 'r', encoding='utf-8') as f:
            all_configs = yaml.safe_load(f)
        self.period = period
        self.config = all_configs[period]
        self.data_sources = all_configs.get('data_sources', {})

    def get_path(self, key: str, path_type: Optional[str] = None, **kwargs) -> Path:
        """根据配置键和可选参数动态生成并返回一个完整的文件路径。

        该方法会从配置文件中获取路径模板，使用提供的关键字参数填充它。

        Args:
            key (str): 配置中路径模板的键名。
            path_type (str, optional): 路径所在的配置块（如 'input_paths'）。
            **kwargs: 用于格式化路径模板的占位符参数。

        Returns:
            Path: 生成的完整文件路径对象。
        """
        if path_type is None:
            template = self.config[key]
        else:
            template = self.config[path_type][key]
        # 格式化路径模板并创建Path对象
        path = Path(template.format(**kwargs))
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_data_source_path(self, key: str, date: str) -> Path:
        """生成并返回指定数据源的文件路径。

        Args:
            key (str): data_sources配置中的数据源键名。
            date (str): 用于格式化路径模板的日期字符串 (YYYYMMDD)。

        Returns:
            Path: 数据源文件的完整路径。
        """
        template = self.data_sources[key]
        return Path(template.format(date=date))

    @staticmethod
    def get_weekly_dates():
        """
        计算周刊相关日期
        
        Returns:
            dict: 包含新旧数据日期和目标日期的字典
            {
                'new_date': 新数据日期(YYYYMMDD),
                'old_date': 旧数据日期(YYYYMMDD),
                'target_date': 命名标记(YYYY-MM-DD),
                'previous_date': 上期标记(YYYY-MM-DD)
            }
        """
        today = datetime.now()
        # 计算距离上一个周六（weekday=5）的天数，并获取该日期
        new_day = today - timedelta(days=(today.weekday() - 5 + 7) % 7)
        # 上一期是再往前推7天
        old_day = new_day - timedelta(days=7)
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d'),
            "target_date": new_day.strftime('%Y-%m-%d'),
            "previous_date": old_day.strftime('%Y-%m-%d')
        }

    @staticmethod
    def get_history_dates() -> dict:
        """获取历史回顾所需的相关日期。

        以当前周的周六为基准，计算52周前的对应日期。
        
        Returns:
            dict: 包含当前和52周前日期的字典
                - old_date: 52周前的日期(YYYY-MM-DD)
                - target_date: 当前周六日期(YYYY-MM-DD)
        """
        today = datetime.now()
        # 获取当前周的周六日期
        now_day = today - timedelta(days=(today.weekday() - 5 + 7) % 7)
        history_day = now_day - timedelta(weeks=52)
        
        return {
            'old_date': history_day.strftime('%Y-%m-%d'),
            'target_date': now_day.strftime('%Y-%m-%d')
        }
    
    @staticmethod
    def get_monthly_dates():
        """计算并返回月刊所需的相关日期。
        
        Returns:
            dict: 包含月度日期信息的字典
            {
                'new_date': 当月日期(YYYYMMDD),
                'old_date': 上月日期(YYYYMMDD),
                'target_date': 命名标记(YYYY-MM),
                'previous_date': 上期标记(YYYY-MM)
            }
        """
        # 本期数据的截止日期是当月1日
        new_day = datetime.now().replace(day=1)
        # 用于文件命名的月份是上个月
        new_month = new_day - timedelta(days=1)
        # 上期数据的截止日期是上个月1日
        old_day = new_month.replace(day=1)
        # 用于文件命名的上个周期是上上个月
        old_month = old_day - relativedelta(months=1)
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d'),
            "target_date": new_month.strftime('%Y-%m'),
            "previous_date": old_month.strftime('%Y-%m')
        }
    
    @staticmethod
    def get_daily_dates():
        """计算并返回日刊所需的相关日期。
        
        以昨天为基准，计算日增数据的统计周期。
        
        Returns:
            dict: 包含日期信息的字典
            {
                'new_date': 新数据日期(YYYYMMDD),
                'old_date': 旧数据日期(YYYYMMDD)
            }
        """
        # 日刊统计的起始时间是前一天的零点
        now_day = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        # 新数据是基准时间后一天（即今天）
        new_day = now_day + timedelta(days=1)
        # 旧数据是基准时间（即昨天）
        old_day = now_day
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d')
        }
    
    @staticmethod
    def get_daily_new_song_dates():
        """计算并返回每日新曲榜所需的相关日期。

        以昨天为基准，提供用于对比排名的三个连续日期。
        
        Returns:
            dict: 包含三个时间点的字典
            {
                'new_date': 新数据日期(YYYYMMDD),
                'now_date': 当前日期(YYYYMMDD),
                'old_date': 旧数据日期(YYYYMMDD)
            }
        """
        # 基准时间是前一天的零点
        now_day = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        # 新数据是今天
        new_day = now_day + timedelta(days=1)
        # 旧数据是前天，用于与昨天的榜单对比排名
        old_day = now_day - timedelta(days=1)
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "now_date": now_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d')
        }
