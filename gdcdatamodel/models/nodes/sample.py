from node import *


class Sample(Node):
    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def sample_type_id(self):
        return self.properties['sample_type_id']

    @sample_type_id.setter
    def sample_type_id(self, value):
        self.properties['sample_type_id'] = value

    @hybrid_property
    def sample_type(self):
        return self.properties['sample_type']

    @sample_type.setter
    def sample_type(self, value):
        self.properties['sample_type'] = value

    @hybrid_property
    def tumor_code_id(self):
        return self.properties['tumor_code_id']

    @tumor_code_id.setter
    def tumor_code_id(self, value):
        self.properties['tumor_code_id'] = value

    @hybrid_property
    def tumor_code(self):
        return self.properties['tumor_code']

    @tumor_code.setter
    def tumor_code(self, value):
        self.properties['tumor_code'] = value

    @hybrid_property
    def longest_dimension(self):
        return self.properties['longest_dimension']

    @longest_dimension.setter
    def longest_dimension(self, value):
        self.properties['longest_dimension'] = value

    @hybrid_property
    def intermediate_dimension(self):
        return self.properties['intermediate_dimension']

    @intermediate_dimension.setter
    def intermediate_dimension(self, value):
        self.properties['intermediate_dimension'] = value

    @hybrid_property
    def shortest_dimension(self):
        return self.properties['shortest_dimension']

    @shortest_dimension.setter
    def shortest_dimension(self, value):
        self.properties['shortest_dimension'] = value

    @hybrid_property
    def initial_weight(self):
        return self.properties['initial_weight']

    @initial_weight.setter
    def initial_weight(self, value):
        self.properties['initial_weight'] = value

    @hybrid_property
    def current_weight(self):
        return self.properties['current_weight']

    @current_weight.setter
    def current_weight(self, value):
        self.properties['current_weight'] = value

    @hybrid_property
    def freezing_method(self):
        return self.properties['freezing_method']

    @freezing_method.setter
    def freezing_method(self, value):
        self.properties['freezing_method'] = value

    @hybrid_property
    def oct_embedded(self):
        return self.properties['oct_embedded']

    @oct_embedded.setter
    def oct_embedded(self, value):
        self.properties['oct_embedded'] = value

    @hybrid_property
    def time_between_clamping_and_freezing(self):
        return self.properties['time_between_clamping_and_freezing']

    @time_between_clamping_and_freezing.setter
    def time_between_clamping_and_freezing(self, value):
        self.properties['time_between_clamping_and_freezing'] = value

    @hybrid_property
    def time_between_excision_and_freezing(self):
        return self.properties['time_between_excision_and_freezing']

    @time_between_excision_and_freezing.setter
    def time_between_excision_and_freezing(self, value):
        self.properties['time_between_excision_and_freezing'] = value

    @hybrid_property
    def days_to_collection(self):
        return self.properties['days_to_collection']

    @days_to_collection.setter
    def days_to_collection(self, value):
        self.properties['days_to_collection'] = value

    @hybrid_property
    def days_to_sample_procurement(self):
        return self.properties['days_to_sample_procurement']

    @days_to_sample_procurement.setter
    def days_to_sample_procurement(self, value):
        self.properties['days_to_sample_procurement'] = value

    @hybrid_property
    def is_ffpe(self):
        return self.properties['is_ffpe']

    @is_ffpe.setter
    def is_ffpe(self, value):
        self.properties['is_ffpe'] = value

    @hybrid_property
    def pathology_report_uuid(self):
        return self.properties['pathology_report_uuid']

    @pathology_report_uuid.setter
    def pathology_report_uuid(self, value):
        self.properties['pathology_report_uuid'] = value
