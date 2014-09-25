#!/usr/bin/python

import logging
# logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s' )
logging.basicConfig(level = logging.INFO, format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s' )

from app.datamodelSync import DatamodelSync
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
sync = DatamodelSync()

@sched.scheduled_job('interval', seconds = 5)
def run():
    sync.run()

if __name__ == '__main__':
    sched.start()
