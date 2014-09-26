#!/usr/bin/python
import logging
from app.cghub_signpost_sync import CGHubSignpostSync
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
sync = CGHubSignpostSync()

@sched.scheduled_job('interval', minutes = 60)
def run():
    sync.run()

if __name__ == '__main__':
    run()
#    sched.start()
