#!/usr/bin/python

import os
import logging
import argparse
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level = logging.INFO, format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s' )
logger = logging.getLogger(name = "[{name}]".format(name = __name__))
sched = BlockingScheduler()

DEFAULT_INTERVAL  = {'minutes': 1}

from zug import Zug, baseDir, callables
from zug.settings import Settings

def setup():

    parser = argparse.ArgumentParser(description='Zug workload distributor.')
    parser.add_argument('--settings', '-s', action="store", default='settings.yaml')
    args = parser.parse_args()

    settingsDir  = os.path.dirname(baseDir)
    settingsPath = os.path.join(settingsDir, args.settings)
    settings = Settings(settingsPath)

    return settings

if __name__ == '__main__':

    settings = setup()
    zug = Zug(settings, callables)
    zug.run()

    if settings.get('schedule', False):
        interval = settings.get('interval', DEFAULT_INTERVAL)
        scheduler.add_job(zug.run, 'interval', **interval)
        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass
