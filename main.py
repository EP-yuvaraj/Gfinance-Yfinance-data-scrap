from threading import Thread
from time import sleep
from app.leadmgmt.livestock import addHistory, addHistoryYahoo,addLive, addLiveYahoo

import time

from apscheduler.schedulers.background import BackgroundScheduler


scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

import schedule




class Live(Thread):
    def run(self):
        job=scheduler.add_job(addLive,'cron',minute="*/3",id='my_job_id')
        scheduler.start()

        while True:
            time.sleep(2)

class History(Thread):
    def run(self):
        schedule.every().day.at("00:13").do(addHistory)

        while True:
            schedule.run_pending()
            time.sleep(1)

class YLive(Thread):
    def run(self):
        schedule.every(4).minutes.do(addLiveYahoo)

        while True:
            schedule.run_pending()
            time.sleep(1)


class YHistory(Thread):
    def run(self):
        schedule.every().day.at("00:05").do(addHistoryYahoo)

        while True:
            schedule.run_pending()
            time.sleep(1)

t1=Live()
t2=History()
t3=YLive()
t4=YHistory()

t1.start()
sleep(2)
t2.start()
sleep(2)
t3.start()
sleep(2)
t4.start()