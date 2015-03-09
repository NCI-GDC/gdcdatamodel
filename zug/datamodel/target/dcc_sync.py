import os
from cdisutils import get_logger
from urlparse import urljoin
import requests
import re
from lxml import html

from zug.datamodel.target.classification import CLASSIFICATION


def tree_walk(url, **kwargs):
    """
    Recursively walk the target html file server, yielding the urls of
    actual files (as opposed to directories).
    """
    resp = requests.get(url, **kwargs)
    resp.raise_for_status()
    elem = html.fromstring(resp.content)
    links = [link for link in elem.cssselect("td a")
             if link.text != "Parent Directory"]
    for link in links:
        fulllink = urljoin(url, link.attrib["href"])
        if "CGI" in fulllink or "CBIIT" in fulllink:
            continue  # skip these for now
        if not fulllink.endswith("/"):
            yield fulllink
        else:
            for file in tree_walk(urljoin(url, link.attrib["href"]), **kwargs):
                yield file


def get_in(data_dict, keys):
    return reduce(lambda d, k: d[k], keys, data_dict)


def classify(path):
    filename = path.pop()
    potential_classifications = get_in(CLASSIFICATION, path)
    for regex, classification in potential_classifications.iteritems():
        if re.match(regex, filename):
            return classification


class TARGETDCCProjectSyncer(object):

    def __init__(self, project, signpost=None,
                 graph=None, dcc_auth=None,
                 storage_client=None, pool=None):
        self.project = project
        # TODO this won't work for AML-P1 / P2
        self.base_url = "https://target-data.nci.nih.gov/{}/".format(project)
        self.signpost = signpost
        self.graph = graph
        self.pool = pool
        self.log = get_logger("taget_dcc_sync_" +
                              str(os.getpid()) +
                              "_" + self.project)

    def file_links(self):
        """A generator for links to files in this project"""
        for toplevel in ["Discovery", "Validation"]:
            url = urljoin(self.base_url, toplevel)
            for file in tree_walk(url, auth=self.dcc_auth):
                yield file

    def process_url(url):
        """Process a url to a target file, allocating an id for it, inserting
        in the database, classifying it, and uploading it from the
        target dcc to our object store
        """
