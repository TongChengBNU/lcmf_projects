#coding=utf8


import schedule
import time
import os
import datetime
import threading

def perform_command(cmd):

    def os_perform(cmd):
        print(cmd, ' -- start at : ', datetime.datetime.now())
        os.system(cmd)
        print(cmd, ' -- end at : ', datetime.datetime.now())

    threading.Thread(target=os_perform, args=(cmd,)).start()


schedule.every(1).minutes.do(perform_command, 'cd /home/jiaoyang/recommend_model/asset_allocation_v2 && bash cppi_bash.sh')
schedule.every(1).minutes.do(perform_command, 'cd /home/jiaoyang/recommend_model/asset_allocation_v2 && bash cppi_bash.sh')


while True:
    schedule.run_pending()
    time.sleep(1)
