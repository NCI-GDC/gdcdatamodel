import abc, logging

class PipelinePluginBase:


    def __init__(self, **kwargs):
        self.docs = []
        self.state = {}

        self.kwargs = kwargs
        self.initialize(**kwargs)

    def initialize(self, **kwargs):
        pass

    def load(self, doc, **state):
        self.docs = []
        for key, value in state.iteritems():
            self.state[key] = value

        try:
            self.docs.append(doc)
        except:
            self.docs = [doc]

        self.start()

    def start(self):
        pass

    def close(self):
        pass
        
