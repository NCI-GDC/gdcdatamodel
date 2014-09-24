import abc

class Conversion(object):

    @abc.abstractmethod
    def initialize(self, **kwargs):
        """ 
        Over-ride this function to initialize your Conversion converter
        """
        raise NotImplementedError("Conversion initialize() not overridden")

    def __init__(self, **kwargs):
        self.ignoreSchedulers  = kwargs.pop('ignoreSchedulers', [])
        self.initialize(**kwargs)

    def _convert(self, doc, **kwargs):
        """
        Calls the child convert() if ignores not triggered
        """
        scheduler  = kwargs.pop('schedulerPlugin', '')
        if scheduler in self.ignoreSchedulers:
            return None
        return self.convert(doc, **kwargs)

    @abc.abstractmethod
    def convert(self, doc, **kwargs):
        raise NotImplementedError("Conversion convert() not overridden")
