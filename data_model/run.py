#!/usr/bin/python

import logging

logging.basicConfig(level = logging.INFO, format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s' )
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

from app.etl import ETL
from app import settings
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
sync = ETL()

@sched.scheduled_job('interval', **settings.get('interval', {'minutes': 5}))
def run():
    sync.run()

if __name__ == '__main__':
    sync.run()
    if settings.get('schedule', False): sched.start()
