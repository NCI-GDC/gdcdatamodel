from psqlgraph import Node, pg_property


class Portion(Node):

    __nonnull_properties__ = [
        'submitter_id', 'portion_number', 'creation_datetime']

    @pg_property(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @pg_property(str)
    def portion_number(self, value):
        self._set_property('portion_number', value)

    @pg_property(long, int)
    def creation_datetime(self, value):
        self._set_property('creation_datetime', value)

    @pg_property(float)
    def weight(self, value):
        self._set_property('weight', value)

    @pg_property(bool)
    def is_ffpe(self, value):
        self._set_property('is_ffpe', value)
