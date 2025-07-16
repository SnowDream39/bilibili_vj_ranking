# utils/config_handler.py
# 配置处理器模块：管理项目配置和日期处理
from pathlib import Path
import yaml
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class ConfigHandler:
    """
    配置处理器类
    负责管理项目配置、路径生成和日期计算
    
    主要功能:
    1. 读取YAML配置文件
    2. 动态生成文件路径
    3. 处理不同周期(日/周/月)的日期逻辑
    """
    def __init__(self, period: str):
        """
        初始化配置处理器
        
        Args:
            period: 周期类型(daily/weekly/monthly)
        """
        # 读取配置文件
        with open('config/rankings.yaml', 'r', encoding='utf-8') as f:
            all_configs = yaml.safe_load(f)
        self.period = period 
        self.config = all_configs[period]
        self.data_sources = all_configs.get('data_sources', {})

    def get_path(self, key: str, path_type: str = None, **kwargs) -> Path:
        """
        生成完整的文件路径
        
        算法流程:
        1. 从配置中获取路径模板
        2. 使用kwargs填充模板中的占位符
        3. 确保父目录存在
        
        Args:
            key: 配置键名
            path_type: 路径类型(默认'output_paths')
            **kwargs: 用于填充路径模板的参数
        
        Returns:
            Path: 生成的完整路径
        """
        if path_type is None:
            template = self.config[key]
        else:
            template = self.config[path_type][key]
        path = Path(template.format(**kwargs))
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_data_source_path(self, key: str, date: str) -> Path:
        """
        生成数据源文件路径
        
        Args:
            key: 数据源键名
            date: 日期字符串
        
        Returns:
            Path: 数据源文件的完整路径
        """
        template = self.data_sources[key]
        return Path(template.format(date=date))

    @staticmethod
    def get_weekly_dates():
        """
        计算周刊相关日期
        
        计算逻辑:
        1. 调整到最近的周六(5)
        2. 计算本期和上期的日期
        
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
        # 按周六标记
        new_day = today - timedelta(days=(today.weekday() - 5 + 7) % 7)
        old_day = new_day - timedelta(days=7)
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d'),
            "target_date": new_day.strftime('%Y-%m-%d'),
            "previous_date": old_day.strftime('%Y-%m-%d')
        }

    @staticmethod
    def get_history_dates() -> dict:
        """
        获取历史回顾的相关日期
        
        Returns:
            dict: 包含当前和52周前日期的字典
                - old_date: 52周前的日期(YYYYMMDD)
                - target_date: 当前周六日期(YYYY-MM-DD)
        """
        today = datetime.now()
        now_day = today - timedelta(days=(today.weekday() - 5 + 7) % 7)
        history_day = now_day - timedelta(weeks=52)
        
        return {
            'old_date': history_day.strftime('%Y-%m-%d'),
            'target_date': now_day.strftime('%Y-%m-%d')
        }
    
    @staticmethod
    def get_monthly_dates():
        """
        计算月刊相关日期
        
        计算逻辑:
        1. 定位到当月1号
        2. 往前推一天得到上月最后一天
        3. 定位到上月1号
        4. 再往前推一个月
        
        Returns:
            dict: 包含月度日期信息的字典
            {
                'new_date': 当月日期(YYYYMMDD),
                'old_date': 上月日期(YYYYMMDD),
                'target_date': 命名标记(YYYY-MM),
                'previous_date': 上期标记(YYYY-MM)
            }
        """
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
        """
        计算日刊相关日期
        
        计算逻辑:
        1. 基准时间为前一天0点
        2. 新数据为基准时间+1天
        3. 旧数据为基准时间
        
        Returns:
            dict: 包含日期信息的字典
            {
                'new_date': 新数据日期(YYYYMMDD),
                'old_date': 旧数据日期(YYYYMMDD)
            }
        """
        now_day = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        new_day = now_day + timedelta(days=1)
        old_day = now_day
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d')
        }
    
    @staticmethod
    def get_daily_new_song_dates():
        """
        计算每日新曲相关日期
        
        计算逻辑:
        1. 基准时间为前一天0点
        2. 新数据为基准时间+1天
        3. 当前数据为基准时间
        4. 旧数据为基准时间-1天
        
        Returns:
            dict: 包含三个时间点的字典
            {
                'new_date': 新数据日期(YYYYMMDD),
                'now_date': 当前日期(YYYYMMDD),
                'old_date': 旧数据日期(YYYYMMDD)
            }
        """
        now_day = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        new_day = now_day + timedelta(days=1)
        old_day = now_day - timedelta(days=1)
        return {
            "new_date": new_day.strftime('%Y%m%d'),
            "now_date": now_day.strftime('%Y%m%d'),
            "old_date": old_day.strftime('%Y%m%d')
        }
    
   