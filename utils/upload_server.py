# utils/upload_server.py
import paramiko
import os
import datetime
from utils.logger import logger

def connect_ssh(host, port, username, password):
    """建立与远程服务器的SSH连接。"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, port, username, password, timeout=10)
        return ssh
    except Exception as e:
        logger.error(f"SSH连接失败: {e}")
        return None

def connect_sftp(ssh):
    """从SSH连接获取SFTP客户端 """
    try:
        sftp = ssh.open_sftp()
        return sftp
    except Exception as e:
        logger.error(f"SFTP通道建立失败: {e}")
        return None

def upload_files(sftp, local_files, remote_path):
    """通过SFTP将本地文件列表上传到远程服务器。"""
    results = {'success': [], 'failed': []}
    for local_path in local_files:
        if os.path.exists(local_path):
            filename = os.path.basename(local_path)
            try:
                remote_file = f"{remote_path}/{filename}"
                sftp.put(local_path, remote_file)
                logger.info(f"成功上传: {filename} 到 {remote_path}")
                results['success'].append(local_path)
            except Exception as e:
                logger.error(f"上传失败: {filename}，错误: {e}")
                results['failed'].append(local_path)
        else:
            logger.error(f"文件不存在，跳过: {local_path}")
            results['failed'].append(local_path)
    return results

def download_files_from_map(sftp, download_maps):
    """下载文件"""
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    logger.info(f"开始执行下载任务，日期标识: {today_str}")
    results = {'success': [], 'failed': []}
    for item in download_maps:
        remote_path = item['remote'].format(date=today_str)
        local_path = item['local'].format(date=today_str)

        try:
            local_dir = os.path.dirname(local_path)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            try:
                sftp.stat(remote_path)
            except FileNotFoundError:
                logger.warning(f"远程文件不存在: {remote_path}")
                results['failed'].append(remote_path)
                continue

            sftp.get(remote_path, local_path)
            logger.info(f"下载成功: {os.path.basename(remote_path)} -> {local_path}")
            results['success'].append(local_path)

        except Exception as e:
            logger.error(f"下载出错 {remote_path}: {e}")
            results['failed'].append(remote_path)
            
    return results

def execute_remote_command(ssh, command):
    """在远程服务器上执行指定的命令。"""
    _, stdout, stderr = ssh.exec_command(command)
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().decode('utf-8', errors='ignore')
    error = stderr.read().decode('utf-8', errors='ignore') if exit_status != 0 else None
    
    return {
        'exit_status': exit_status,
        'output': output,
        'error': error
    }

def close_connections(sftp=None, ssh=None):
    """安全关闭连接"""
    if sftp: sftp.close()
    if ssh: ssh.close()
    logger.info("连接已关闭")
