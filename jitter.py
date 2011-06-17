
import sys
import time

SLEEP = 5

while 1:
    a = time.time()
    time.sleep(SLEEP)
    b = time.time()
    sys.stdout.write(str(b-a)+'\n')
    sys.stdout.flush()
