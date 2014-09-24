import os, imp

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('Scheduler', basePath)

class Scheduler(base.Scheduler):

    def __init__(self, **kwargs):
        pass

    def load(self, **kwargs):
        pass

    def next(self, **kwargs):
        pass

    def close(self, **kwargs):
        pass
        
