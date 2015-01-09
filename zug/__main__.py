#!/usr/bin/python

import os
import logging
import argparse
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()

DEFAULT_INTERVAL = {'minutes': 1}

from __init__ import Zug, baseDir
from settings import Settings


def setup():
    defaultSettingsPath = os.path.join(os.getcwdu(), 'settings.yaml')

    parser = argparse.ArgumentParser(description='Zug workload distributor.')
    parser.add_argument('--settings', '-s', action="store",
                        default=defaultSettingsPath)
    args = parser.parse_args()

    settingsDir = os.path.dirname(baseDir)
    settingsPath = os.path.join(settingsDir, args.settings)
    settings = Settings(settingsPath)

    return settings

if __name__ == '__main__':

    settings = setup()

    zug = Zug(settings)
    zug.run()

    if settings.get('schedule', False):
        interval = settings.get('interval', DEFAULT_INTERVAL)
        scheduler.add_job(zug.run, 'interval', **interval)
        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass
