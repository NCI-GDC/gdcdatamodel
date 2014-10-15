import os
import imp
from zug import Zug, basePlugin

from settings import Settings

baseDir = os.path.dirname(os.path.realpath(__file__))

class Callables():

    def __init__(self):
        self.callables = {}
 
    def register(self):
        def func_wrapper(func):
            name = func.__name__
            self.callables[name] = func
        return func_wrapper

    def __getitem__(self, key):
        return self.callables['key']

callables = Callables()
