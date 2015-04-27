from psqlgraph import Node, pg_property


class ExperimentalStrategy(Node):

    __label__ = 'experimental_strategy'
    __nonnull_properties__ = ['name']

    @pg_property(str)
    def name(self, value):
        self._set_property('name',  value)
