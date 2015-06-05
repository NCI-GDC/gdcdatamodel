from psqlgraph import Node, pg_property


class DataFormat(Node):

    __label__ = 'data_format'
    __nonnull_properties__ = ['name']

    @pg_property(str)
    def name(self, value):
        self._set_property('name', value)
