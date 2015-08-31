from psqlgraph import Node, pg_property


class Sample(Node):

    __nonnull_properties__ = ['sample_type_id', 'sample_type']

    @pg_property(str)
    def submitter_id(self, value):
        return self._set_property('submitter_id', value)

    @pg_property(str, enum=[
        '01', '02', '03', '04', '05', '06', '07', '08', '09',
        '10', '11', '12', '13', '14', '15', '20', '40', '41',
        '42', '50', '60', '61', '99'])
    def sample_type_id(self, value):
        return self._set_property('sample_type_id', value)

    @pg_property(str, enum=[
        'Blood Derived Normal', 'Buccal Cell Normal',
        'Control Analyte', 'DNA', 'EBV Immortalized Normal',
        'GenomePlex (Rubicon) Amplified DNA',
        'Granulocytes',
        'Primary Blood Derived Cancer - Peripheral Blood',
        'Recurrent Blood Derived Cancer - Peripheral Blood',
        'Primary Tumor',
        'Recurrent Blood Derived Cancer - Bone Marrow',
        'Recurrent Tumor', 'Repli-G (Qiagen) DNA',
        'Repli-G X (Qiagen) DNA',
        'RNA', 'Slides', 'Solid Tissue Normal', 'Total RNA',
        'Metastatic', 'Additional - New Primary',
        'Additional Metastatic',
        'Human Tumor Original Cells',
        'Post neo-adjuvant therapy',
        'Primary Blood Derived Cancer - Bone Marrow',
        'Blood Derived Cancer - Bone Marrow, Post-treatment',
        'Blood Derived Cancer - Peripheral Blood, Post-treatment',
        'Cell Lines', 'Bone Marrow Normal',
        'Primary Xenograft Tissue',
        'Cell Line Derived Xenograft Tissue',
        'Fibroblasts from Bone Marrow Normal'])
    def sample_type(self, value):
        return self._set_property('sample_type', value)

    @pg_property(str, enum=[
        '00', '01', '02', '03', '04', '10', '20', '21', '30',
        '40', '41', '50', '51', '52', '60', '61', '62', '63',
        '64', '65', '70', '71', '80', '81'])
    def tumor_code_id(self, value):
        return self._set_property('tumor_code_id', value)

    @pg_property(str, enum=[
        'Non cancerous tissue',
        'Diffuse Large B-Cell Lymphoma (DLBCL)',
        "Lung Cancer (all types)",
        "Cervical Cancer (all types)",
        "Anal Cancer (all types)",
        "Acute lymphoblastic leukemia (ALL)",
        "Acute myeloid leukemia (AML)",
        "Induction Failure AML (AML-IF)",
        "Neuroblastoma (NBL)", "Osteosarcoma (OS)",
        "Ewing sarcoma", "Wilms tumor (WT)",
        "Clear cell sarcoma of the kidney (CCSK)",
        "Rhabdoid tumor (kidney) (RT)",
        "CNS, ependymoma", "CNS, glioblastoma (GBM)",
        "CNS, rhabdoid tumor",
        "CNS, low grade glioma (LGG)",
        "CNS, medulloblastoma",  "CNS, other",
        "NHL, anaplastic large cell lymphoma",
        "NHL, Burkitt lymphoma (BL)", "Rhabdomyosarcoma",
        "Soft tissue sarcoma, non-rhabdomyosarcoma"])
    def tumor_code(self, value):
        return self._set_property('tumor_code', value)

    @pg_property(str)
    def longest_dimension(self, value):
        return self._set_property('longest_dimension', value)

    @pg_property(str)
    def intermediate_dimension(self, value):
        return self._set_property('intermediate_dimension', value)

    @pg_property(str)
    def shortest_dimension(self, value):
        return self._set_property('shortest_dimension', value)

    # note (jjp, 8/10/15) that these next two are intended to be
    # floats going forward, the str is only there for backwards
    # compatibility and should eventually be removed

    @pg_property(float, str)
    def initial_weight(self, value):
        return self._set_property('initial_weight', value)

    @pg_property(float, str)
    def current_weight(self, value):
        return self._set_property('current_weight', value)

    @pg_property(str)
    def freezing_method(self, value):
        return self._set_property('freezing_method', value)

    @pg_property(str)
    def oct_embedded(self, value):
        return self._set_property('oct_embedded', value)

    @pg_property(str)
    def time_between_clamping_and_freezing(self, value):
        return self._set_property('time_between_clamping_and_freezing', value)

    @pg_property(str)
    def time_between_excision_and_freezing(self, value):
        return self._set_property('time_between_excision_and_freezing', value)

    @pg_property(int)
    def days_to_collection(self, value):
        return self._set_property('days_to_collection', value)

    @pg_property(int)
    def days_to_sample_procurement(self, value):
        return self._set_property('days_to_sample_procurement', value)

    @pg_property(bool)
    def is_ffpe(self, value):
        return self._set_property('is_ffpe', value)

    @pg_property(str)
    def pathology_report_uuid(self, value):
        return self._set_property('pathology_report_uuid', value)
