from node import *


class Portion(Node):
    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def portion_number(self):
        return self.properties['portion_number']

    @portion_number.setter
    def portion_number(self, value):
        self.properties['portion_number'] = value

    @hybrid_property
    def creation_datetime(self):
        return self.properties['creation_datetime']

    @creation_datetime.setter
    def creation_datetime(self, value):
        self.properties['creation_datetime'] = value

    @hybrid_property
    def weight(self):
        return self.properties['weight']

    @weight.setter
    def weight(self, value):
        self.properties['weight'] = value

    @hybrid_property
    def is__ffpe(self):
        return self.properties['is__ffpe']

    @is__ffpe.setter
    def is__ffpe(self, value):
        self.properties['is__ffpe'] = value
