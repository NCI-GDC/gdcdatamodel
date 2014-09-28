#!/usr/bin/python
import validator

from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()

@sched.scheduled_job('interval', seconds = 30)
def run():
  validator.validate()

if __name__ == '__main__':
  #run()
  sched.start()
