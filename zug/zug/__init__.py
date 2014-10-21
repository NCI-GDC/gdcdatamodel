import os
import imp
from zug import Zug, basePlugin, exceptions

from settings import Settings

baseDir = os.path.dirname(os.path.realpath(__file__))

# Functions below are used as decorators 

def process(f):
    f.zug_process = True
    return f

def initialize(f):
    f.zug_initialize = True
    return f
