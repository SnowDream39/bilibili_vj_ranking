# utils/formatters.py
"""通用格式化工具模块，提供文本清理、时间格式化等功能。"""
import re

def clean_tags(text: str) -> str:
    """清理字符串中的HTML标签和不可见特殊字符。

    Args:
        text (str): 待清理的字符串。

    Returns:
        str: 清理后的字符串。
    """
    # 使用正则表达式移除HTML标签
    text = re.sub(r'<.*?>', '', text)
    # 移除各种不可见的控制字符和空白字符
    return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F\u200B-\u200F\u2028-\u202F\u205F-\u206F\uFEFF\uFFFE-\uFFFF]','', text)

def convert_duration(duration: int) -> str:
    """将秒数转换为"M分S秒"或"S秒"的格式。
    
    Args:
        duration (int): 视频时长（秒）。
    
    Returns:
        str: 格式化的时长字符串。
    """
    # B站的视频时长通常是向上取整，这里减1秒可以更准确地反映分钟数
    minutes, seconds = divmod(duration - 1, 60)
    return f'{minutes}分{seconds}秒' if minutes > 0 else f'{seconds}秒'

