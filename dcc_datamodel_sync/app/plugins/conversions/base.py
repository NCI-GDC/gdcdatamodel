import abc

class Conversion:

    @abc.abstractmethod
    def __init__(self, **kwargs):
        pass

    @abc.abstractmethod
    def convert(self, doc, **kwargs):
        pass
        
