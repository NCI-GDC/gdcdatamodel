import psqlgraph


class CheckDatamodel(object):

    """
    """

    def __init__(self, host, user, password, database,
                 node_validator=None):
        """Walks the graph to produce elasticsearch json documents.
        Assumptions include:

        """
        self.graph = psqlgraph.PsqlGraphDriver(
            host=host, user=user, password=password, database=database)

    def get_case_count(self):
        return self.graph.node_lookup(label='case').count()
