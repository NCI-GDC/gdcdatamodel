#!/usr/bin/python
import logging
from app.tcgadcc_signpost_sync import TCGADCCSignpostSync
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
sync = TCGADCCSignpostSync()

@sched.scheduled_job('interval', minutes = 60)
def run():
    sync.run()

if __name__ == '__main__':
    sched.add_job(run)
    sched.start()
