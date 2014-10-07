import os, imp, requests, logging, yaml
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)
logger     = logging.getLogger(name = "[{name}]".format(name = __name__))

bcr = imp.load_source('bcr', os.path.join(currentDir, 'bcr.py'))

class PipelinePlugin(base.PipelinePluginBase):

    """
    datamodel pipeline stage
    """

    def initialize(self, **kwargs):

        assert 'translate_path' in kwargs, "Please specify path to translate.yml"
        assert 'data_type' in kwargs, "Please specify data_type (i.e. biospecimen)"

        with open(kwargs['translate_path']) as f: self.translate = yaml.load(f)
        self.bp = bcr.BiospecimenParser(self.translate[kwargs['data_type']])
    
    def next(self, doc):
        self.bp.parse_biospecimen(data = doc)
        self.bp.print_graph()
        return self.bp
    
