import os, imp

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('Scheduler', basePath)

# Signpost scheduler

class Scheduler(base.Scheduler):

    """
    Default signpost scheduler
    """

    def initialize(self, **kwargs):
        self.docs = range(10)

    def __iter__(self):
        for doc in self.docs:
            yield doc

    def load(self, **kwargs):
        pass

    def next(self, **kwargs):
        pass

    def close(self, **kwargs):
        pass
        
