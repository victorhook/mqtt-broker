import subprocess
import os

"""
    Helper for rc service.
    Probably a horrible way to kill a program but
    this it works. I don't know how to write proper
    services files for the rc service handler..
"""

proc = subprocess.Popen(['sockstat'], stdout=subprocess.PIPE) 
stdout = proc.stdout.read().decode()

for result in stdout.splitlines():
    if ':1883' in result:
        pid = result.split()[2]
        os.system(f'kill {pid}')
