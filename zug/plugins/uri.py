import logging
import urllib
import os

from lxml import etree
from pprint import pprint
from zug import basePlugin
from zug.exceptions import IgnoreDocumentException, EndOfQueue

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))


class uri(basePlugin):

    def process(self, doc):
        data = urllib.urlopen(doc).read().strip()
        if self.kwargs.get('split', False):
            data = data.split(self.kwargs.get('delimiter', '\n'))
            for res in data: 
                self.yieldDoc(res)
            raise IgnoreDocumentException()
        else:
            return data
    

