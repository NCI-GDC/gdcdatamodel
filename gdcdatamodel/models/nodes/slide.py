from node import *

class Slide(Node):
    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def section_location(self):
        return self.properties['section_location']

    @section_location.setter
    def section_location(self, value):
        self.properties['section_location'] = value

    @hybrid_property
    def number_proliferating_cells(self):
        return self.properties['number_proliferating_cells']

    @number_proliferating_cells.setter
    def number_proliferating_cells(self, value):
        self.properties['number_proliferating_cells'] = value

    @hybrid_property
    def percent_tumor_cells(self):
        return self.properties['percent_tumor_cells']

    @percent_tumor_cells.setter
    def percent_tumor_cells(self, value):
        self.properties['percent_tumor_cells'] = value

    @hybrid_property
    def percent_tumor_nuclei(self):
        return self.properties['percent_tumor_nuclei']

    @percent_tumor_nuclei.setter
    def percent_tumor_nuclei(self, value):
        self.properties['percent_tumor_nuclei'] = value

    @hybrid_property
    def percent_normal_cells(self):
        return self.properties['percent_normal_cells']

    @percent_normal_cells.setter
    def percent_normal_cells(self, value):
        self.properties['percent_normal_cells'] = value

    @hybrid_property
    def percent_necrosis(self):
        return self.properties['percent_necrosis']

    @percent_necrosis.setter
    def percent_necrosis(self, value):
        self.properties['percent_necrosis'] = value

    @hybrid_property
    def percent_stromal_cells(self):
        return self.properties['percent_stromal_cells']

    @percent_stromal_cells.setter
    def percent_stromal_cells(self, value):
        self.properties['percent_stromal_cells'] = value

    @hybrid_property
    def percent_inflam_infiltration(self):
        return self.properties['percent_inflam_infiltration']

    @percent_inflam_infiltration.setter
    def percent_inflam_infiltration(self, value):
        self.properties['percent_inflam_infiltration'] = value

    @hybrid_property
    def percent_lymphocyte_infiltration(self):
        return self.properties['percent_lymphocyte_infiltration']

    @percent_lymphocyte_infiltration.setter
    def percent_lymphocyte_infiltration(self, value):
        self.properties['percent_lymphocyte_infiltration'] = value

    @hybrid_property
    def percent_monocyte_infiltration(self):
        return self.properties['percent_monocyte_infiltration']

    @percent_monocyte_infiltration.setter
    def percent_monocyte_infiltration(self, value):
        self.properties['percent_monocyte_infiltration'] = value

    @hybrid_property
    def percent_granulocyte_infiltration(self):
        return self.properties['percent_granulocyte_infiltration']

    @percent_granulocyte_infiltration.setter
    def percent_granulocyte_infiltration(self, value):
        self.properties['percent_granulocyte_infiltration'] = value

    @hybrid_property
    def percent_neutrophil_infiltration(self):
        return self.properties['percent_neutrophil_infiltration']

    @percent_neutrophil_infiltration.setter
    def percent_neutrophil_infiltration(self, value):
        self.properties['percent_neutrophil_infiltration'] = value

    @hybrid_property
    def percent_eosinophil_infiltration(self):
        return self.properties['percent_eosinophil_infiltration']

    @percent_eosinophil_infiltration.setter
    def percent_eosinophil_infiltration(self, value):
        self.properties['percent_eosinophil_infiltration'] = value
