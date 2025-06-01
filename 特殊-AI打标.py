from datetime import datetime, timedelta
import pandas as pd
from utils.tagger import Tagger
from utils.io_utils import save_to_excel, format_columns
from utils.tagger import Tagger, ZJUConfig, CozeConfig
import yaml

with open("config/ai.yaml", 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)
    config = CozeConfig(**config)
    tagger = Tagger(config)

today = (datetime.now()-timedelta(days=1)).replace(hour=0, minute=0,second=0,microsecond=0).strftime('%Y%m%d')
file_name = f"特殊/特殊原始数据/梦的结唱5"

songs_data = pd.read_excel(f"{file_name}.xlsx", dtype={'name': str, 'type':str, 'author':str, 'synthesizer': str ,'vocal': str})

tagger.tagging(songs_data)

save_to_excel(format_columns(songs_data), f"{file_name}.xlsx")