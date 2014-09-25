import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('Scheduler', basePath)

# File scheduler

class Scheduler(base.Scheduler):

    """
    File scheduler
    """

    prefix = "Scheduler: file: " 

    def initialize(self, **kwargs):

        args = ['path']
        for arg in args:
            assert arg in kwargs, "Please specify argument [{arg}] in settings yaml under 'file:'".format(arg = arg)

        self.docs = []
        self.urls = []

        self.path = kwargs['path']


    def __iter__(self):
        for url in self.urls:
            local = url.split('/')[-1]
            logging.info(self.prefix + "pulling file {url}".format(url = local))

            response = requests.get(url)

            doc = response.text
            schedulerDetails = {'url': url}

            yield (doc, schedulerDetails)

    def load(self, **kwargs):
        with open(self.path) as f:
            self.urls = f.readlines()

    def close(self, **kwargs):
        pass
        
