from node import *
from gdcdatamodel.models import validate


class Slide(Node):

    __nonnull_properties__ = ['submitter_id', 'section_location']

    @hybrid_property
    def submitter_id(self):
        return self._get_property('submitter_id')

    @submitter_id.setter
    @validate(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @hybrid_property
    def section_location(self):
        return self._get_property('section_location')

    @section_location.setter
    @validate(str)
    def section_location(self, value):
        self._set_property('section_location', value)

    @hybrid_property
    def number_proliferating_cells(self):
        return self._get_property('number_proliferating_cells')

    @number_proliferating_cells.setter
    @validate(long, int)
    def number_proliferating_cells(self, value):
        self._set_property('number_proliferating_cells', value)

    @hybrid_property
    def percent_tumor_cells(self):
        return self._get_property('percent_tumor_cells')

    @percent_tumor_cells.setter
    @validate(float)
    def percent_tumor_cells(self, value):
        self._set_property('percent_tumor_cells', value)

    @hybrid_property
    def percent_tumor_nuclei(self):
        return self._get_property('percent_tumor_nuclei')

    @percent_tumor_nuclei.setter
    @validate(float)
    def percent_tumor_nuclei(self, value):
        self._set_property('percent_tumor_nuclei', value)

    @hybrid_property
    def percent_normal_cells(self):
        return self._get_property('percent_normal_cells')

    @percent_normal_cells.setter
    @validate(float)
    def percent_normal_cells(self, value):
        self._set_property('percent_normal_cells', value)

    @hybrid_property
    def percent_necrosis(self):
        return self._get_property('percent_necrosis')

    @percent_necrosis.setter
    @validate(float)
    def percent_necrosis(self, value):
        self._set_property('percent_necrosis', value)

    @hybrid_property
    def percent_stromal_cells(self):
        return self._get_property('percent_stromal_cells')

    @percent_stromal_cells.setter
    @validate(float)
    def percent_stromal_cells(self, value):
        self._set_property('percent_stromal_cells', value)

    @hybrid_property
    def percent_inflam_infiltration(self):
        return self._get_property('percent_inflam_infiltration')

    @percent_inflam_infiltration.setter
    @validate(float)
    def percent_inflam_infiltration(self, value):
        self._set_property('percent_inflam_infiltration', value)

    @hybrid_property
    def percent_lymphocyte_infiltration(self):
        return self._get_property('percent_lymphocyte_infiltration')

    @percent_lymphocyte_infiltration.setter
    @validate(float)
    def percent_lymphocyte_infiltration(self, value):
        self._set_property('percent_lymphocyte_infiltration', value)

    @hybrid_property
    def percent_monocyte_infiltration(self):
        return self._get_property('percent_monocyte_infiltration')

    @percent_monocyte_infiltration.setter
    @validate(float)
    def percent_monocyte_infiltration(self, value):
        self._set_property('percent_monocyte_infiltration', value)

    @hybrid_property
    def percent_granulocyte_infiltration(self):
        return self._get_property('percent_granulocyte_infiltration')

    @percent_granulocyte_infiltration.setter
    @validate(float)
    def percent_granulocyte_infiltration(self, value):
        self._set_property('percent_granulocyte_infiltration', value)

    @hybrid_property
    def percent_neutrophil_infiltration(self):
        return self._get_property('percent_neutrophil_infiltration')

    @percent_neutrophil_infiltration.setter
    @validate(float)
    def percent_neutrophil_infiltration(self, value):
        self._set_property('percent_neutrophil_infiltration', value)

    @hybrid_property
    def percent_eosinophil_infiltration(self):
        return self._get_property('percent_eosinophil_infiltration')

    @percent_eosinophil_infiltration.setter
    @validate(float)
    def percent_eosinophil_infiltration(self, value):
        self._set_property('percent_eosinophil_infiltration', value)
