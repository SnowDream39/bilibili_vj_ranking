import pandas as pd
from utils.tagger import Tagger
import yaml

with open("ai_config.yaml", 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)
    tagger = Tagger(**config)

file_name = "测试内容/新曲20250326与新曲20250325"

songs_data = pd.read_excel(f"{file_name}.xlsx", dtype={'name': str, 'type':str, 'author':str, 'synthesizer': str ,'vocal': str})

tagger.tagging(songs_data)

songs_data.to_excel(f"{file_name}打标结果.xlsx", index=False)