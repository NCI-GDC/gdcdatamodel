import abc

class PipelinePluginBase:

    def __init__(self, **kwargs):
        pass

    def initialize(self, **kwargs):
        pass

    def __iter__(self):
        pass

    def start(self, doc = None):
        pass

    def close(self):
        pass
        
