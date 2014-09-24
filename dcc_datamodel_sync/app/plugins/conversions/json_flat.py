import os, imp

currentDir   = os.path.dirname(os.path.realpath(__file__))
basePath     = os.path.join(currentDir, 'base.py')
base         = imp.load_source('Conversion', basePath)
xml2jsonPath = os.path.join(currentDir, 'xml2json.py')
xml2json     = imp.load_source('xml2json', xml2jsonPath)

class Conversion(base.Conversion):

    def __init__(self, **kwargs):
        self.conv = xml2json.xml2json()

    def convert(self, doc, **kwargs):
        self.conv.loadFromString(str(doc))
        doc = self.conv.toJSON(flatten=True)
        return doc
        
