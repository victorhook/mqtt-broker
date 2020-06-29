import subprocess
import os

proc = subprocess.Popen(['sockstat'], stdout=subprocess.PIPE) 
stdout = proc.stdout.read().decode()

for result in stdout.splitlines():
    if ':1883' in result:
        pid = result.split()[2]
        os.system(f'kill {pid}')
