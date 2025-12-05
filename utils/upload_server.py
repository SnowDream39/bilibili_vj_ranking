# utils.upload_server.py
# SSH/SFTP工具模块，用于连接远程服务器、上传文件和执行命令。
import paramiko
import os
from utils.logger import logger

def connect_ssh(host, port, username, password):
    """建立与远程服务器的SSH连接。

    Args:
        host (str): 服务器主机名或IP地址。
        port (int): SSH端口。
        username (str): 登录用户名。
        password (str): 登录密码。

    Returns:
        paramiko.SSHClient: 成功时返回SSH客户端对象，失败时返回None。
    """
    # 创建SSH客户端实例
    ssh = paramiko.SSHClient()
    # 自动添加服务器的主机密钥，简化首次连接
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, port, username, password, timeout=10)
        return ssh
    except Exception:
        return None

def upload_files(sftp, local_files, remote_path):
    """通过SFTP将本地文件列表上传到远程服务器的指定目录。

    Args:
        sftp (paramiko.SFTPClient): 已建立的SFTP客户端。
        local_files (list): 包含本地文件路径的列表。
        remote_path (str): 远程服务器上的目标目录路径。

    Returns:
        dict: 包含'success'和'failed'两个列表，分别记录上传成功和失败的本地文件路径。
    """
    results = {'success': [], 'failed': []}
    for local_path in local_files:
        if os.path.exists(local_path):
            filename = os.path.basename(local_path)
            try:
                # 构建远程文件的完整路径
                remote_file = f"{remote_path}/{filename}"
                # 执行上传操作
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

def execute_remote_command(ssh, command):
    """在远程服务器上执行指定的命令并返回结果。

    Args:
        ssh (paramiko.SSHClient): 已建立的SSH客户端。
        command (str): 要在远程服务器上执行的命令字符串。

    Returns:
        dict: 包含命令执行结果的字典，包括'exit_status', 'output', 'error'。
    """
    # 执行命令，获取标准输入、输出和错误流
    _, stdout, stderr = ssh.exec_command(command)
    # 等待命令执行完成并获取退出状态码
    exit_status = stdout.channel.recv_exit_status()
    # 读取标准输出
    output = stdout.read().decode('utf-8', errors='ignore')
    # 仅在命令执行失败时读取标准错误
    error = stderr.read().decode('utf-8', errors='ignore') if exit_status != 0 else None
    
    logger.info("远程命令执行完毕。")
    return {
        'exit_status': exit_status,
        'output': output,
        'error': error
    }

def run_task(ssh, sftp, local_files, remote_command, remote_path):
    """执行一个完整的远程任务：上传文件，然后执行远程命令。

    Args:
        ssh (paramiko.SSHClient): SSH客户端。
        sftp (paramiko.SFTPClient): SFTP客户端。
        local_files (list): 要上传的本地文件列表。
        remote_command (str): 上传后要执行的远程命令。
        remote_path (str): 远程上传目录。
    """
    # 第一步：上传文件
    upload_files(sftp, local_files, remote_path)
    # 第二步：执行远程命令
    command_result = execute_remote_command(ssh, remote_command)
    logger.info(command_result['output'])
    if command_result['exit_status'] != 0:
        logger.error("错误信息：", command_result['error'])

def close_connections(sftp=None, ssh=None):
    """安全地关闭SFTP和SSH连接。

    Args:
        sftp (paramiko.SFTPClient, optional): 要关闭的SFTP客户端。
        ssh (paramiko.SSHClient, optional): 要关闭的SSH客户端。
    """
    if sftp:
        sftp.close()
    if ssh:
        ssh.close()

