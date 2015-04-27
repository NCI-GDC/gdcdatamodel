from psqlgraph import Node, pg_property


class Tag(Node):

    __nonnull_properties__ = ['name']

    @pg_property(str)
    def name(self, value):
        self._set_property('name', value)
