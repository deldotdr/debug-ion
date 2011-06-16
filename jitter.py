
import time

SLEEP = 5

while 1:
    a = time.time()
    time.sleep(SLEEP)
    b = time.time()
    print b-a
