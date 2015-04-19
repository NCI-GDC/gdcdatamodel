from node import *
from gdcdatamodel.models import validate


class Clinical(Node):

    __nonnull_properties__ = []

    @hybrid_property
    def gender(self):
        return self._get_property('gender')

    @gender.setter
    @validate(str, enum=['male', 'femle'])
    def gender(self, value):
        self._set_property('gender', value)

    @hybrid_property
    def race(self):
        return self._get_property('race')

    @race.setter
    @validate(str, enum=['not reported', 'white',
                         'american indian or alaska native',
                         'black or african american',
                         'asian',
                         'native hawaiian or other pacific islander',
                         'other'])
    def race(self, value):
        self._set_property('race', value)

    @hybrid_property
    def ethnicity(self):
        return self._get_property('ethnicity')

    @ethnicity.setter
    @validate(str, enum=['hispanic or latino', 'not hispanic or latino'])
    def ethnicity(self, value):
        self._set_property('ethnicity', value)

    @hybrid_property
    def vital_status(self):
        return self._get_property('vital_status')

    @vital_status.setter
    @validate(str, enum=['alive', 'dead', 'lost to follow-up'])
    def vital_status(self, value):
        self._set_property('vital_status', value)

    @hybrid_property
    def year_of_diagnosis(self):
        return self._get_property('year_of_diagnosis')

    @year_of_diagnosis.setter
    @validate(int)
    def year_of_diagnosis(self, value):
        self._set_property('year_of_diagnosis', value)

    @hybrid_property
    def age_at_diagnosis(self):
        return self._get_property('age_at_diagnosis')

    @age_at_diagnosis.setter
    @validate(int)
    def age_at_diagnosis(self, value):
        self._set_property('age_at_diagnosis', value)

    @hybrid_property
    def days_to_death(self):
        return self._get_property('days_to_death')

    @days_to_death.setter
    @validate(int)
    def days_to_death(self, value):
        self._set_property('days_to_death', value)

    @hybrid_property
    def icd_10(self):
        return self._get_property('icd_10')

    @icd_10.setter
    @validate(str)
    def icd_10(self, value):
        self._set_property('icd_10', value)
