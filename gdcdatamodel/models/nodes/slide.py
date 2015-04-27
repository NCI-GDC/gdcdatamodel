from psqlgraph import Node, pg_property


class Slide(Node):

    __nonnull_properties__ = ['submitter_id', 'section_location']

    @pg_property(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @pg_property(str)
    def section_location(self, value):
        self._set_property('section_location', value)

    @pg_property(long, int)
    def number_proliferating_cells(self, value):
        self._set_property('number_proliferating_cells', value)

    @pg_property(float)
    def percent_tumor_cells(self, value):
        self._set_property('percent_tumor_cells', value)

    @pg_property(float)
    def percent_tumor_nuclei(self, value):
        self._set_property('percent_tumor_nuclei', value)

    @pg_property(float)
    def percent_normal_cells(self, value):
        self._set_property('percent_normal_cells', value)

    @pg_property(float)
    def percent_necrosis(self, value):
        self._set_property('percent_necrosis', value)

    @pg_property(float)
    def percent_stromal_cells(self, value):
        self._set_property('percent_stromal_cells', value)

    @pg_property(float)
    def percent_inflam_infiltration(self, value):
        self._set_property('percent_inflam_infiltration', value)

    @pg_property(float)
    def percent_lymphocyte_infiltration(self, value):
        self._set_property('percent_lymphocyte_infiltration', value)

    @pg_property(float)
    def percent_monocyte_infiltration(self, value):
        self._set_property('percent_monocyte_infiltration', value)

    @pg_property(float)
    def percent_granulocyte_infiltration(self, value):
        self._set_property('percent_granulocyte_infiltration', value)

    @pg_property(float)
    def percent_neutrophil_infiltration(self, value):
        self._set_property('percent_neutrophil_infiltration', value)

    @pg_property(float)
    def percent_eosinophil_infiltration(self, value):
        self._set_property('percent_eosinophil_infiltration', value)
