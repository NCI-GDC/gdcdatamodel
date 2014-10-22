import os
import logging
import copy 
import sys

from pprint import pprint

from zug import basePlugin

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class tsv2graph(basePlugin):

    """
    converts a tsv to a list of nodes with properties

    :param header: list of strings specifying the key for each column.

    """
    
    def initialize(self, **kwargs):
        assert 'id_field' in kwargs, 'Please add "id" key, value to settings. This maps which column header to be used as the node id'
        assert 'id_field' in kwargs, 'Please add "type" key, value to settings.'
        self.id_field = kwargs['id_field']
        self._type = kwargs['type']

    def process(self, doc):

        graph = {'nodes': {}, 'edges': {}}

        lines = doc.strip().split('\n')
        fields = self.kwargs.get('header', lines[0].split('\t'))
        id_index = fields.index(self.id_field)
        start = self.kwargs.get('skip_rows', 1)

        for line in lines[start:]: 
            sline = line.strip().split('\t')
            id = sline[id_index]
            node = dict(zip(fields, sline))

            node['_type'] = self._type
            node['id'] = id
            
            graph['nodes'][id] = node

        return graph

            


