from psqlgraph import Node, pg_property


class Program(Node):

    @pg_property(str)
    def name(self, value):
        self._set_property('name', value)
