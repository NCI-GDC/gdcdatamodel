import os, imp

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('Scheduler', basePath)

# Signpost scheduler

class Scheduler(base.Scheduler):

    """
    Default signpost scheduler
    """


    def __init__(self, **kwargs):
        self.docs = []

    def __iter__(self):
        for attr in dir(self):
            if not attr.startswith("__"):
                yield attr

    def load(self, **kwargs):
        pass

    def next(self, **kwargs):
        pass

    def close(self, **kwargs):
        pass
        
