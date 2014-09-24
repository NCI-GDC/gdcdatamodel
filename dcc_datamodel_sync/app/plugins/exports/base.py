import abc

class Export:

    @abc.abstractmethod
    def __init__(self, **kwargs):
        pass

    @abc.abstractmethod
    def export(self, doc, **kwargs):
        pass

    @abc.abstractmethod
    def close(self, **kwargs):
        pass
        
