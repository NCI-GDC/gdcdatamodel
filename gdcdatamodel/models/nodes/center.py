from node import *


class Center(Node):
    @hybrid_property
    def code(self):
        return self.properties['code']

    @code.setter
    def code(self, value):
        self.properties['code'] = value

    @hybrid_property
    def namespace(self):
        return self.properties['namespace']

    @namespace.setter
    def namespace(self, value):
        self.properties['namespace'] = value

    @hybrid_property
    def name(self):
        return self.properties['name']

    @name.setter
    def name(self, value):
        self.properties['name'] = value

    @hybrid_property
    def short_name(self):
        return self.properties['short_name']

    @short_name.setter
    def short_name(self, value):
        self.properties['short_name'] = value

    @hybrid_property
    def center_type(self):
        return self.properties['center_type']

    @center_type.setter
    def center_type(self, value):
        self.properties['center_type'] = value
