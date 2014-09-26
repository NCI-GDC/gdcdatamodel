#!/usr/bin/python
import logging
from app.tcgadcc_import_reporter import TCGADCCImportReporter
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
sync = TCGADCCImportReporter()

@sched.scheduled_job('interval', minutes = 30)
def run():
    sync.run()

if __name__ == '__main__':
    sched.start()
