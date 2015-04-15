from node import *


class Aliquot(Node):
    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def source_center(self):
        return self.properties['source_center']

    @source_center.setter
    def source_center(self, value):
        self.properties['source_center'] = value

    @hybrid_property
    def amount(self):
        return self.properties['amount']

    @amount.setter
    def amount(self, value):
        self.properties['amount'] = value

    @hybrid_property
    def concentration(self):
        return self.properties['concentration']

    @concentration.setter
    def concentration(self, value):
        self.properties['concentration'] = value
