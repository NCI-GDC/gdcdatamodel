from node import *


class Archive(Node):
    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def revision(self):
        return self.properties['revision']

    @revision.setter
    def revision(self, value):
        self.properties['revision'] = value
