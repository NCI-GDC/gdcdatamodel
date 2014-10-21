import os
import imp
import requests
import logging
import re
import tarfile
import urllib
import sys

from datetime import datetime, tzinfo, timedelta

from zug import basePlugin

logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class extract_tar(basePlugin):

    """
    extract_xml.py
    Opens a uri to tarfile and returns any extracted xml docs
    """

    def initialize(self, uris = [], mode = "r|gz", regex = None, **kwargs):

        self.mode = mode

        if regex:
            self.pattern = re.compile(regex)
        else:
            self.pattern = None

    def process(self, doc):

        stream = urllib.urlopen(doc)
        tfile = tarfile.open(fileobj=stream, mode=self.mode)

        for entry in tfile:
            if not self.pattern or self.pattern.match(entry.name):
                self.finished(tfile.extractfile(entry).read())
