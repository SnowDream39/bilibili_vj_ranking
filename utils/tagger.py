import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from utils.real_name import find_original_name
from dataclasses import dataclass
from abc import ABC
from sseclient import SSEClient
from utils.logger import logger
from utils.retry_handler import RetryHandler
class ApiConfig(ABC):
    API_URL: str
    
@dataclass
class CozeConfig(ApiConfig):
    API_URL: str
    API_KEY: str
    BOT_ID: str
    USER_ID: str

@dataclass
class ZJUConfig(ApiConfig):
    TYPE = 'zju'
    API_URL: str
    API_KEY: str
    USER_ID: str

def byte_stream(response: requests.Response):
    try:
        for chunk in response.iter_content(chunk_size=2048):
            yield chunk
    except:
        # 不要抛出异常
        pass

class Tagger:
    def __init__(
            self,
            apiConfig: ApiConfig
        ):
        self.apiConfig = apiConfig

        if type(self.apiConfig) == CozeConfig:
            self.headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.apiConfig.API_KEY}"
            }
        elif type(self.apiConfig) == ZJUConfig:
            self.headers = {
                "Content-Type": "application/json",
                "Apikey": f"{self.apiConfig.API_KEY}"
            }
        else:
            raise ValueError("Invalid apiConfig")

    def chat(self, content: str) -> str:
        if type(self.apiConfig) == CozeConfig:
            data = {
                "bot_id": self.apiConfig.BOT_ID,
                "user_id": self.apiConfig.USER_ID,
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

            response = requests.post(self.apiConfig.API_URL, headers=self.headers, json=data, stream=True)
            if response.status_code == 200:
                client = SSEClient(byte_stream(response))
                for event in client.events():
                    if event.event == 'conversation.message.completed':
                        return json.loads(event.data)['content']
                else: 
                    raise Exception(f"Error: {response.status_code}")
            else:
                raise Exception(f"请求失败，状态码：{response.status_code}")        
            
        elif type(self.apiConfig) == ZJUConfig:
            data = {
                "AppKey": self.apiConfig.API_KEY,
                "UserId": self.apiConfig.USER_ID,
            }
            response = requests.post('https://open.zju.edu.cn/api/proxy/api/v1/create_conversation', headers=self.headers, json=data)
            appId: str = response.json()['Conversation']['AppConversationID']
            data = {
                "AppKey": self.apiConfig.API_KEY,
                "AppConversationID": appId,
                "UserId": self.apiConfig.USER_ID,
                "Query": content,
                "ResponseMode": 'blocking'
            }

            response = requests.post('https://open.zju.edu.cn/api/proxy/api/v1/chat_query', headers=self.headers, json=data, stream=True)
            if response.status_code == 200:
                client = SSEClient(byte_stream(response))
                for event in client.events():
                    # 实际上只有一次，因为用了blocking
                    return json.loads(event.data.split(':', 1)[1])['answer']
                else:
                    raise Exception(f'Error: {response.status_code}')
            else:
                raise Exception(f"请求失败，状态码：{response.status_code}")        
        else:
            raise Exception("不合法的api_config")
    
    def result2json(self, text: str) -> list:
        return json.loads(text.split("```")[1])

    
    def df2prompt(self, df: pd.DataFrame) -> str:
        result = ""
        records: list = json.loads(df.to_json(orient="records"))
        for index, record in enumerate(records):
            result += (   
                f'# 视频{index+1}\n'
                '## 标题\n'
                f'{record["title"]}\n'
                '## 上传者\n'
                f'{record["uploader"]}\n'
                '## 标签\n'
                f'{record["tags"]}\n'
                '## 简介\n'
                f'{record["description"]}\n\n'
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
                if songs.loc[i, ['type', 'synthesizer', 'vocal']].isna().all():
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
        def action() -> list:
            prompt = self.df2prompt(songs_part)
            result = self.chat(prompt)
            logger.info(songs_part.index[0])
            result = self.result2json(result)
            return result

        handler = RetryHandler()
        result = handler.retry(action)
        return result

    # =========================== 完整工作流函数 ====================================
    def chat_info(self, songs:pd.DataFrame) -> list:
        length = len(songs.index)
        results = [None] * ((length + 9) // 10)

        with ThreadPoolExecutor(max_workers=20) as executor:
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
        info_list = self.chat_info(songs)

        logger.info("AI打标完成，正在填入数据...")
        self.to_real_name(info_list)
        self.fill_info(songs, info_list)


            
