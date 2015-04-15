from node import *


class DataFormat(Node):
    __label__ = 'data_format'

    @hybrid_property
    def name(self):
        return self.properties['name']

    @name.setter
    def name(self, value):
        self.properties['name'] = value

