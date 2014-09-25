import abc

class Scheduler:

    @abc.abstractmethod
    def __init__(self, **kwargs):
        self.initialize(**kwargs)

    def initialize(self, **kwargs):
        raise NotImplementedError("Scheduler initialize() not overridden")

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def load(self, **kwargs):
        pass

    @abc.abstractmethod
    def next(self, **kwargs):
        pass

    @abc.abstractmethod
    def close(self, **kwargs):
        pass
        
