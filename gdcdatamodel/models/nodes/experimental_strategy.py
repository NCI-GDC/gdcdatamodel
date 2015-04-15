from node import *


class ExperimentalStrategy(Node):
    __label__ = 'experimental_strategy'

    @hybrid_property
    def name(self):
        return self.properties['name']

    @name.setter
    def name(self, value):
        self.properties['name'] = value
