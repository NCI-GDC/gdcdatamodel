from node import *


class Participant(Node):
    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def days_to_index(self):
        return self.properties['days_to_index']

    @days_to_index.setter
    def days_to_index(self, value):
        self.properties['days_to_index'] = value
