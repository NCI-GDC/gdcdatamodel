from node import *


class DataSubtype(Node):
    __label__ = 'data_subtype'

    @hybrid_property
    def name(self):
        return self.properties['name']

    @name.setter
    def name(self, value):
        self.properties['name'] = value
