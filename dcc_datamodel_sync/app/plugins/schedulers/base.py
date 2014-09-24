import abc

class Scheduler:

    @abc.abstractmethod
    def __init__(self, **kwargs):
        pass

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
        
