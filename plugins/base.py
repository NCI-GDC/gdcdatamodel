import abc, logging

class PipelinePluginBase:

    def __init__(self, **kwargs):
        self.docs = []
        self.state = {}
        self.kwargs = kwargs
        self.initialize(**kwargs)

    def initialize(self, **kwargs):
        pass

    def load(self, __doc__, **__state__):
        self.docs = []

        for key, value in __state__.iteritems():
            self.state[key] = value
        try:
            self.docs.append(__doc__)
        except:
            self.docs = [__doc__]


    def __iter__(self):
        for doc in self.docs:
            if doc is None:
                logging.warn("Stage was passed a NoneType doc")
                continue
            yield self.next(doc)

    def next(self, doc):
        return doc

    def start(self):
        pass

    def close(self):
        pass
        
