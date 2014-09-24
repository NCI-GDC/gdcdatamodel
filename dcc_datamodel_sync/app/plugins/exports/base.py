import abc

class Export(object):


    def __init__(self, **kwargs):
        self.ignoreSchedulers  = kwargs.pop('ignoreSchedulers', [])
        self.ignoreConversions = kwargs.pop('ignoreConversions', [])
        self.initialize(**kwargs)

    @abc.abstractmethod
    def initialize(self, **kwargs):
        raise NotImplementedError("Export initialize() not overridden")

    def _export(self, doc, **kwargs):
        scheduler  = kwargs.pop('schedulerPlugin', '')
        conversion = kwargs.pop('conversionPlugin', '')


        if scheduler in self.ignoreSchedulers:
            return None
        if conversion in self.ignoreConversions:
            return None

        return self.export(doc, **kwargs)

    @abc.abstractmethod
    def export(self, doc, **kwargs):
        pass

    @abc.abstractmethod
    def close(self, **kwargs):
        pass
        
