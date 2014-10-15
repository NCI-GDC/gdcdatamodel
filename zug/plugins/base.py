
class ZugPluginBase(object):

    def __init__(self, **kwargs):

        self.docs = []
        self.state = {}
        self.kwargs = kwargs
        self.initialize(**kwargs)

    def initialize(self, **kwargs):
        pass

    def load(self, __doc__, **__state__):
        for key, value in __state__.iteritems():
            self.state[key] = value
        self.docs = [__doc__]

    def __iter__(self):
        for doc in self.docs:
            yield self.next(doc)

    def next(self, doc):
        return doc

    def start(self):
        pass

    def close(self):
        pass
        
