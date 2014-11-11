import os
import imp
import logging
from zug import Zug, basePlugin, exceptions

from settings import Settings

baseDir = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger(name="[zug]")

# Functions below are used as decorators 

def no_proxy(func):
    def wrapped(*args, **kwargs):
        http_proxy = os.environ.get('http_proxy', None)
        https_proxy = os.environ.get('https_proxy', None)

        logger.info("no_proxy: " + str(func))

        if http_proxy: 
            logger.info("Unsetting http_proxy")
            del os.environ['http_proxy']

        if https_proxy: 
            logger.info("Unsetting https_proxy")
            del os.environ['https_proxy']
            
        ret = func(*args, **kwargs) 

        if http_proxy:
            logger.info("Resetting http_proxy: " + http_proxy)
            os.environ['http_proxy'] = http_proxy

        if https_proxy:
            logger.info("Resetting https_proxy: " + https_proxy)
            os.environ['https_proxy'] = https_proxy

        return ret
    return wrapped

def process(f):
    f.zug_process = True
    return f

def initialize(f):
    f.zug_initialize = True
    return f
