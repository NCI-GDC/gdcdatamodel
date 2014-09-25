import abc

class Export(object):

    def __init__(self, **kwargs):
        self.ignoreSchedulers  = kwargs.pop('ignoreSchedulers', [])
        self.ignoreConversions = kwargs.pop('ignoreConversions', [])
        self.onlySchedulers  = kwargs.pop('onlySchedulers', None)
        self.onlyConversions = kwargs.pop('onlyConversions', None)
        self.initialize(**kwargs)

    @abc.abstractmethod
    def initialize(self, **kwargs):
        raise NotImplementedError("Export initialize() not overridden")

    def _export(self, doc, schedulerDetails, conversionDetails, **kwargs):
        scheduler  = kwargs.pop('schedulerPlugin', '')
        conversion = kwargs.pop('conversionPlugin', '')

        if scheduler in self.ignoreSchedulers:
            return None

        if conversion in self.ignoreConversions:
            return None

        if self.onlySchedulers and scheduler not in self.onlySchedulers:
            return None

        if self.onlyConversions and conversion not in self.onlyConversions:
            print 'skipping'
            return None

        return self.export(doc, schedulerDetails, conversionDetails, **kwargs)

    @abc.abstractmethod
    def export(self, doc, schedulerDetails, conversionDetails, **kwargs):
        pass

    @abc.abstractmethod
    def close(self, **kwargs):
        pass
        
