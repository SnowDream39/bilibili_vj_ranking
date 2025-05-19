import pandas as pd
from utils.tagger import Tagger, ZJUConfig, CozeConfig
import yaml

with open("config/ai.yaml", 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)
    config = CozeConfig(**config)
    tagger = Tagger(config)

file_name = "测试内容/梦的结唱4"

songs_data = pd.read_excel(f"{file_name}.xlsx", dtype={'name': str, 'type':str, 'author':str, 'synthesizer': str ,'vocal': str})

tagger.tagging(songs_data)

songs_data.to_excel(f"{file_name}打标结果.xlsx", index=False)