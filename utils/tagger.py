import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from utils.real_name import find_original_name

class Tagger:
    def __init__(
            self,
            API_URL: str, 
            API_KEY: str,
            BOT_ID: int,
            USER_ID: int
        ):
        self.API_URL = API_URL
        self.API_KEY = API_KEY
        self.BOT_ID = BOT_ID
        self.USER_ID = USER_ID

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.API_KEY}"
        }

    def chat(self, content: str) -> str:
        result = ""
        data = {
            "bot_id": self.BOT_ID,
            "user_id": self.USER_ID,
            "stream": True,
            "auto_save_history": True,
            "additional_messages": [
                {
                    "role": "user", 
                    "content": content, 
                    "content_type": "text"
                }
            ],
        }

        response = requests.post(self.API_URL, headers=self.headers, data=json.dumps(data), stream=True)
        
        if response.status_code == 200:
            completed = False
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')

                    if completed:
                        result = json.loads(decoded_line.split(':', 1)[1])["content"]
                        response.close()
                        break

                    if decoded_line == "event:conversation.message.completed":
                        completed = True
        else:
            print(f"Error: {response.status_code}")
            print(response.json())

        return result
    
    def result2json(self, text: str) -> list:
        return json.loads(text.split("```")[1])

    
    def df2prompt(self, df: pd.DataFrame) -> str:
        result = ""
        records: list = json.loads(df.to_json(orient="records"))
        for index, record in enumerate(records):
            result += (   
                f"# 视频{index+1}\n"
                "## 标题\n"
                f"{record["title"]}\n"
                "## 上传者\n"
                f"{record["uploader"]}\n"
                "## 标签\n"
                f"{record["tags"]}\n"
                "## 简介\n"
                f"{record["description"]}\n\n"
            )
        return result
    
    def to_real_name(self, info_list: list):
        for info in info_list:
            if info['isSong']:
                info['info']['vocal'] = "、".join(map(find_original_name, info['info']['vocal'].split('、')))
    
    def fill_info(self, songs: pd.DataFrame, info_list: list):
        for i in range(len(info_list)):
            if info_list[i]['isSong']:
                info = info_list[i]['info']
                # songs.loc[index, 'name'] = info['name']
                songs.loc[i, 'type'] = info['type']
                # songs.loc[index, 'author'] = info['author']
                songs.loc[i, 'synthesizer'] = info['synthesizer']
                songs.loc[i, 'vocal'] = info['vocal']
                # if songs.loc[index, 'copyright'] == 1 and info['copyright'] == '搬运':
                #     songs.loc[index, 'copyright'] = 3
                # elif songs.loc[index, 'copyright'] == 2 and info['copyright'] == '本家投稿':
                #     songs.loc[index, 'copyright'] = 4

    def chat_info_part(self, songs_part: pd.DataFrame) -> list:
        prompt = self.df2prompt(songs_part)
        result = self.chat(prompt)
        print(songs_part.index[0])
        return self.result2json(result)


    def chat_info(self, songs:pd.DataFrame) -> list:
        length = len(songs.index)
        results = [None] * ((length + 9) // 10)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {}
            for i in range(0, length, 10):
                part = songs.iloc[i:min(i+10, length)]
                batch_idx = i // 10
                future = executor.submit(self.chat_info_part, part)
                futures[future] = batch_idx


            for future in as_completed(futures):
                idx = futures[future]
                results[idx] = future.result()

        return list(chain.from_iterable(results))
            

    def tagging(self, songs: pd.DataFrame):
        length = len(songs.index)
        info_list = self.chat_info(songs)

        print("AI打标完成，正在填入数据...")
        self.to_real_name(info_list)
        self.fill_info(songs, info_list)


            
