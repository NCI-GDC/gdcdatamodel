from node import *


class Program(Node):
    @hybrid_property
    def name(self):
        return self.properties['name']

    @name.setter
    def name(self, value):
        self.properties['name'] = value
