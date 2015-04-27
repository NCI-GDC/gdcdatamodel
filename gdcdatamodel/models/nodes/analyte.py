from psqlgraph import Node, pg_property


class Analyte(Node):

    __nonnull_properties__ = [
        'submitter_id',
        'analyte_type_id',
        'analyte_type',
    ]

    @pg_property(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @pg_property(str, enum=['D', 'G', 'H', 'R', 'T', 'W', 'X'])
    def analyte_type_id(self, value):
        self._set_property('analyte_type_id', value)

    @pg_property(str, enum=[
        'DNA',
        'EBV Immortalized Normal',
        'GenomePlex (Rubicon) Amplified DNA',
        'Repli-G (Qiagen) DNA',
        'Repli-G X (Qiagen) DNA',
        'RNA', 'Total RNA'])
    def analyte_type(self, value):
        self._set_property('analyte_type', value)

    @pg_property(float)
    def concentration(self, value):
        self._set_property('concentration', value)

    @pg_property(float)
    def amount(self, value):
        self._set_property('amount', value)

    @pg_property(float)
    def a260_a280_ratio(self, value):
        self._set_property('a260_a280_ratio', value)

    @pg_property(str)
    def well_number(self, value):
        self._set_property('well_number', value)

    @pg_property(str)
    def spectrophotometer_method(self, value):
        self._set_property('spectrophotometer_method', value)
