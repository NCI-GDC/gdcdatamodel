from node import *


class Clinical(Node):
    @hybrid_property
    def gender(self):
        return self.properties['gender']

    @gender.setter
    def gender(self, value):
        self.properties['gender'] = value

    @hybrid_property
    def race(self):
        return self.properties['race']

    @race.setter
    def race(self, value):
        self.properties['race'] = value

    @hybrid_property
    def ethnicity(self):
        return self.properties['ethnicity']

    @ethnicity.setter
    def ethnicity(self, value):
        self.properties['ethnicity'] = value

    @hybrid_property
    def vital_status(self):
        return self.properties['vital_status']

    @vital_status.setter
    def vital_status(self, value):
        self.properties['vital_status'] = value

    @hybrid_property
    def year_of_diagnosis(self):
        return self.properties['year_of_diagnosis']

    @year_of_diagnosis.setter
    def year_of_diagnosis(self, value):
        self.properties['year_of_diagnosis'] = value

    @hybrid_property
    def days_to_death(self):
        return self.properties['days_to_death']

    @days_to_death.setter
    def days_to_death(self, value):
        self.properties['days_to_death'] = value

    @hybrid_property
    def icd_10(self):
        return self.properties['icd_10']

    @icd_10.setter
    def icd_10(self, value):
        self.properties['icd_10'] = value
