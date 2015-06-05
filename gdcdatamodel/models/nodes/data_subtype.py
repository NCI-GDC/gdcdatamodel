from psqlgraph import Node, pg_property


class DataSubtype(Node):
    __label__ = 'data_subtype'
    __nonnull_properties__ = ['name']

    @pg_property(str)
    def name(self, value):
        self._set_property('name', value)
