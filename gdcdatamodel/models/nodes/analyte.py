from node import *


class Analyte(Node):
    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def analyte_type_id(self):
        return self.properties['analyte_type_id']

    @analyte_type_id.setter
    def analyte_type_id(self, value):
        self.properties['analyte_type_id'] = value

    @hybrid_property
    def analyte_type(self):
        return self.properties['analyte_type']

    @analyte_type.setter
    def analyte_type(self, value):
        self.properties['analyte_type'] = value

    @hybrid_property
    def concentration(self):
        return self.properties['concentration']

    @concentration.setter
    def concentration(self, value):
        self.properties['concentration'] = value

    @hybrid_property
    def amount(self):
        return self.properties['amount']

    @amount.setter
    def amount(self, value):
        self.properties['amount'] = value

    @hybrid_property
    def a260_a280_ratio(self):
        return self.properties['a260_a280_ratio']

    @a260_a280_ratio.setter
    def a260_a280_ratio(self, value):
        self.properties['a260_a280_ratio'] = value

    @hybrid_property
    def well_number(self):
        return self.properties['well_number']

    @well_number.setter
    def well_number(self, value):
        self.properties['well_number'] = value

    @hybrid_property
    def spectrophotometer_method(self):
        return self.properties['spectrophotometer_method']

    @spectrophotometer_method.setter
    def spectrophotometer_method(self, value):
        self.properties['spectrophotometer_method'] = value
