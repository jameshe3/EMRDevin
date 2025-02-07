#!/usr/bin/env python3
import subprocess
import time

def execute_ssh_command(host, password, command, timeout=30, retries=3):
    ssh_cmd = f'sshpass -p "{password}" ssh -o StrictHostKeyChecking=no root@{host} "{command}"'
    for attempt in range(retries):
        try:
            result = subprocess.run(ssh_cmd, shell=True, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                return result.stdout
            time.sleep(2 ** attempt)  # Exponential backoff
        except subprocess.TimeoutExpired:
            if attempt == retries - 1:
                return f"Command timed out after {timeout} seconds"
        except Exception as e:
            if attempt == retries - 1:
                return f"Error executing command: {str(e)}"
    return "Failed after all retries"
