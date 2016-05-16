from datetime import datetime, timedelta
import time

from core import gen


while(True):
    t1 = datetime.now()

    gen_time = datetime(t1.year, t1.month, t1.day, t1.hour, t1.minute, t1.second)
    txns = gen(gen_time)
    txns = sorted(txns, key=lambda item: item[2])

    for item in txns:
        print('{0},{1},{2}'.format(
                item[0],
                item[1],
                datetime.strftime(item[2], '%Y-%m-%d %H:%M:%S.') + str(int(item[2].microsecond / 1000))
                #datetime.strftime(item[2], '%Y-%m-%d %H:%M:%S.%f') #comment above and uncomment if wanna work with python
        ))

    time_to_sleep = 1 - (datetime.now() - t1).total_seconds()
    if time_to_sleep > 0 and time_to_sleep < 1:
        time.sleep(time_to_sleep)
