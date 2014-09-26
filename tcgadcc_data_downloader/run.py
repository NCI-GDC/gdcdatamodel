#!/usr/bin/python
import logging
from app.tcgadcc_data_downloader import TCGADCCDataDownloader
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
sync = TCGADCCDataDownloader()

@sched.scheduled_job('interval', seconds = 5)
def run():
    sync.run()

if __name__ == '__main__':
    sched.start()
