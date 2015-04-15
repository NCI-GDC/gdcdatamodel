from node import *


class DataType(Node):
    __label__ = 'data_type'

    @hybrid_property
    def name(self):
        return self.properties['name']

    @name.setter
    def name(self, value):
        self.properties['name'] = value
