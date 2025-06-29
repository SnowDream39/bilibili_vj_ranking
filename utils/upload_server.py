# utils.upload_server.py
import paramiko
import os

def connect_ssh(host, port, username, password):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, port, username, password, timeout=10)
        return ssh
    except Exception:
        return None

def upload_files(sftp, local_files, remote_path):
    results = {'success': [], 'failed': []}
    for local_path in local_files:
        if os.path.exists(local_path):
            filename = os.path.basename(local_path)
            try:
                remote_file = f"{remote_path}/{filename}"
                sftp.put(local_path, remote_file)
                print(f"成功上传: {filename} 到 {remote_path}")
                results['success'].append(local_path)
            except Exception as e:
                print(f"上传失败: {filename}，错误: {e}")
                results['failed'].append(local_path)
        else:
            print(f"文件不存在，跳过: {local_path}")
            results['failed'].append(local_path)
    return results

def execute_remote_command(ssh, command):
    
    _, stdout, stderr = ssh.exec_command(command)
    exit_status = stdout.channel.recv_exit_status()
    
    output = stdout.read().decode('utf-8', errors='ignore')
    error = stderr.read().decode('utf-8', errors='ignore') if exit_status != 0 else None
    
    print("远程命令执行完毕。")
    return {
        'exit_status': exit_status,
        'output': output,
        'error': error
    }

def run_task(ssh, sftp, local_files, remote_command, remote_path):
    
    upload_files(sftp, local_files, remote_path)
    
    command_result = execute_remote_command(ssh, remote_command)
    print(command_result['output'])
    if command_result['exit_status'] != 0:
        print("错误信息：", command_result['error'])

def close_connections(sftp=None, ssh=None):
    if sftp:
        sftp.close()
    if ssh:
        ssh.close()

