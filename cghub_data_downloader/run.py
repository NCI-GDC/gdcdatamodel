#!/usr/bin/python
import logging
from app.cghub_data_downloader import CGHubDataDownloader
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
sync = CGHubDataDownloader()

@sched.scheduled_job('interval', minutes = 60)
def run():
    sync.run()

if __name__ == '__main__':
    run()
#    sched.start()
