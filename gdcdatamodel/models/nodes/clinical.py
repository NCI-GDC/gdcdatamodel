from psqlgraph import Node, pg_property


class Clinical(Node):

    __nonnull_properties__ = []

    @pg_property(str, enum=['male', 'female'])
    def gender(self, value):
        self._set_property('gender', value)

    @pg_property(str, enum=[
        'not reported',
        'white',
        'american indian or alaska native',
        'black or african american',
        'asian',
        'native hawaiian or other pacific islander',
        'other'])
    def race(self, value):
        self._set_property('race', value)

    @pg_property(str, enum=['hispanic or latino', 'not hispanic or latino'])
    def ethnicity(self, value):
        self._set_property('ethnicity', value)

    @pg_property(str, enum=['alive', 'dead', 'lost to follow-up'])
    def vital_status(self, value):
        self._set_property('vital_status', value)

    @pg_property(int)
    def year_of_diagnosis(self, value):
        self._set_property('year_of_diagnosis', value)

    @pg_property(int)
    def age_at_diagnosis(self, value):
        self._set_property('age_at_diagnosis', value)

    @pg_property(int)
    def days_to_death(self, value):
        self._set_property('days_to_death', value)

    @pg_property(str)
    def icd_10(self, value):
        self._set_property('icd_10', value)
