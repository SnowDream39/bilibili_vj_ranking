import pandas as pd
from utils.tagger import Tagger
import yaml

with open("ai_config.yaml", 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)
    tagger = Tagger(**config)


songs_data = pd.read_excel("测试内容/新曲打标测试.xlsx", dtype={'name': str, 'type':str, 'author':str, 'synthesizer': str ,'vocal': str})

tagger.tagging(songs_data)

songs_data.to_excel("测试内容/新曲打标测试结果.xlsx", index=False)