import os
import imp
from zug import Zug, basePlugin, exceptions

from settings import Settings

baseDir = os.path.dirname(os.path.realpath(__file__))

# Functions below are used as decorators 

def next(f):
    f.zug_next = True
    return f

def initialize(f):
    f.zug_initialize = True
    return f

def start(f):
    f.zug_start = True
    return f

def start(f):
    f.zug___iter__ = True
    return f

