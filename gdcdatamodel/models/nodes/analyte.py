from node import *
from gdcdatamodel.models import validate


class Analyte(Node):

    __nonnull_properties__ = ['submitter_id', 'analyte_type_id',
                              'analyte_type']

    @hybrid_property
    def submitter_id(self):
        return self._get_property('submitter_id')

    @submitter_id.setter
    @validate(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @hybrid_property
    def analyte_type_id(self):
        return self._get_property('analyte_type_id')

    @analyte_type_id.setter
    @validate(str, enum=['D', 'G', 'H', 'R', 'T', 'W', 'X'])
    def analyte_type_id(self, value):
        self._set_property('analyte_type_id', value)

    @hybrid_property
    def analyte_type(self):
        return self._get_property('analyte_type')

    @analyte_type.setter
    @validate(str, enum=['DNA', 'EBV Immortalized Normal',
                         'GenomePlex (Rubicon) Amplified DNA',
                         'Repli-G (Qiagen) DNA',
                         'Repli-G X (Qiagen) DNA',
                         'RNA', 'Total RNA'])
    def analyte_type(self, value):
        self._set_property('analyte_type', value)

    @hybrid_property
    def concentration(self):
        return self._get_property('concentration')

    @concentration.setter
    @validate(float)
    def concentration(self, value):
        self._set_property('concentration', value)

    @hybrid_property
    def amount(self):
        return self._get_property('amount')

    @amount.setter
    @validate(float)
    def amount(self, value):
        self._set_property('amount', value)

    @hybrid_property
    def a260_a280_ratio(self):
        return self._get_property('a260_a280_ratio')

    @a260_a280_ratio.setter
    @validate(float)
    def a260_a280_ratio(self, value):
        self._set_property('a260_a280_ratio', value)

    @hybrid_property
    def well_number(self):
        return self._get_property('well_number')

    @well_number.setter
    @validate(str)
    def well_number(self, value):
        self._set_property('well_number', value)

    @hybrid_property
    def spectrophotometer_method(self):
        return self._get_property('spectrophotometer_method')

    @spectrophotometer_method.setter
    @validate(str)
    def spectrophotometer_method(self, value):
        self._set_property('spectrophotometer_method', value)
