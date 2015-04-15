from node import *


class Project(Node):
    @hybrid_property
    def code(self):
        return self.properties['code']

    @code.setter
    def code(self, value):
        self.properties['code'] = value

    @hybrid_property
    def name(self):
        return self.properties['name']

    @name.setter
    def name(self, value):
        self.properties['name'] = value

    @hybrid_property
    def disease_type(self):
        return self.properties['disease_type']

    @disease_type.setter
    def disease_type(self, value):
        self.properties['disease_type'] = value

    @hybrid_property
    def state(self):
        return self.properties['state']

    @state.setter
    def state(self, value):
        self.properties['state'] = value

    @hybrid_property
    def primary_site(self):
        return self.properties['primary_site']

    @primary_site.setter
    def primary_site(self, value):
        self.properties['primary_site'] = value
