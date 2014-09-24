#!/usr/bin/python

import logging
logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s' )

from app.datamodelSync import DatamodelSync

if __name__ == '__main__':
    sync = DatamodelSync()
    sync.run()
