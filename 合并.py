# 合并.py
import asyncio
import yaml
from src.ranking_processor import RankingProcessor
from utils.upload_server import connect_ssh, upload_files, close_connections

async def main():
    processor = RankingProcessor(period='daily_combination')
    await processor.run()

def upload():
    '''
    上传文件到数据服务器
    '''
    with open("config/上传数据服务器.yaml",encoding='utf-8') as file:
            data = yaml.safe_load(file)
            HOST = data['HOST']
            PORT = data['PORT']
            USERNAME = data['USERNAME']
            PASSWORD = data['PASSWORD']
            REMOTE_PATH = data['REMOTE_PATH']
            local_files = data['local_files']

    ssh = connect_ssh(HOST, PORT, USERNAME, PASSWORD)
    sftp = ssh.open_sftp()  
    upload_files(sftp, local_files, REMOTE_PATH)
    close_connections(sftp, ssh)
    input("上传完成")
    
if __name__ == "__main__":
    asyncio.run(main())
    upload()

