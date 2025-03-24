import requests
import json
import pandas as pd


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
    
    def fill_info(self, songs: pd.DataFrame, info_list: list, start: int, end: int):
        try:
            for i,index in enumerate(range(start, end)):
                if info_list[i]['isSong']:
                    info = info_list[i]['info']
                    songs.loc[index, 'name'] = info['name']
                    songs.loc[index, 'type'] = info['type']
                    songs.loc[index, 'author'] = info['author']
                    songs.loc[index, 'synthesizer'] = info['synthesizer']
                    songs.loc[index, 'vocal'] = info['vocal']
                    if songs.loc[index, 'copyright'] == 1 and info['copyright'] == '搬运':
                        songs.loc[index, 'copyright'] = 3
                    elif songs.loc[index, 'copyright'] == 2 and info['copyright'] == '本家投稿':
                        songs.loc[index, 'copyright'] = 4
        except Exception as e:
            print(f"打标第{start+1}到{end}个视频出错：{e}")
            

    def tagging(self, songs: pd.DataFrame):
        length = len(songs.index)
        for i in range(0, length, 10):
            start = i
            end = min(start+10, length)
            print(f"正在打标第{start+1}到第{end}个视频")
            songs_part = songs.iloc[start:end]
            prompt = self.df2prompt(songs_part)
            result = self.chat(prompt)
            info = self.result2json(result)
            self.fill_info(songs, info, start, end)


            
