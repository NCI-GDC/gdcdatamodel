from node import *


class TissueSourceSite(Node):
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
    def project(self):
        return self.properties['project']

    @project.setter
    def project(self, value):
        self.properties['project'] = value

    @hybrid_property
    def bcr_id(self):
        return self.properties['bcr_id']

    @bcr_id.setter
    def bcr_id(self, value):
        self.properties['bcr_id'] = value
