import re
import tarfile
import urllib2

from cdisutils.log import get_logger

logger = get_logger(__name__)


class ExtractTar(object):

    """
    extract_xml.py
    Opens a uri to tarfile and returns any extracted xml docs
    """

    def __init__(self, uris=[], mode="r|gz", regex=None, **kwargs):
        self.mode = mode
        if regex:
            self.pattern = re.compile(regex)
        else:
            self.pattern = None

    def __call__(self, url):
        stream = urllib2.urlopen(url)
        logger.info(url)
        tfile = tarfile.open(fileobj=stream, mode=self.mode)
        for entry in tfile:
            if not self.pattern or self.pattern.match(entry.name):
                logger.info(entry.name)
                yield tfile.extractfile(entry).read()
