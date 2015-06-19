from psqlgraph import Node, pg_property


class Case(Node):
    __tablename__ = '_case'
    __nonnull_properties__ = ['submitter_id']

    @pg_property(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @pg_property(int)
    def days_to_index(self, value):
        self._set_property('days_to_index', value)
