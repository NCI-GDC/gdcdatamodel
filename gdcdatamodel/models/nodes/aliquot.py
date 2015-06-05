from psqlgraph import Node, pg_property


class Aliquot(Node):

    __nonnull_properties__ = ['submitter_id']

    @pg_property(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @pg_property(str)
    def source_center(self, value):
        self._set_property('source_center', value)

    @pg_property(float)
    def amount(self, value):
        self._set_property('amount', value)

    @pg_property(float)
    def concentration(self, value):
        self._set_property('concentration', value)
