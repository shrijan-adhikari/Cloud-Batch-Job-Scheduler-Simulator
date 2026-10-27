import os
import sys
import shutil
import subprocess
import time

tmp_exe = os.path.join(os.environ['TEMP'], 'train-resnet.exe')
shutil.copyfile(sys.executable, tmp_exe)
p = subprocess.Popen([tmp_exe, '-c', 'import time; time.sleep(2)'])
try:
    res = subprocess.check_output(f'tasklist /FI "PID eq {p.pid}"', shell=True).decode()
    print("TASKLIST OUTPUT:")
    print(res)
finally:
    p.wait()
    if os.path.exists(tmp_exe):
        os.remove(tmp_exe)
