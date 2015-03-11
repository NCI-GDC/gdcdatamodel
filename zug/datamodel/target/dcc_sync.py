import os
from cdisutils.log import get_logger
from urlparse import urljoin
import requests
import re
import hashlib
from lxml import html

from psqlgraph import PsqlNode

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
    # TODO need to downcase
    potential_classifications = get_in(CLASSIFICATION, path)
    for regex, classification in potential_classifications.iteritems():
        if re.match(regex, filename):
            return classification


class MD5SummingStream(object):

    def __init__(self, stream):
        self.md5 = hashlib.md5()
        self.stream = stream

    def __iter__(self):
        for chunk in self.stream:
            self.md5.update(chunk)
            yield chunk

    def hexdigest(self):
        return self.md5.hexdigest()


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

    @property
    def container(self):
            return self.storage_client.get_container("target_dcc_protected")

    def file_links(self):
        """A generator for links to files in this project"""
        for toplevel in ["Discovery", "Validation"]:
            url = urljoin(self.base_url, toplevel)
            for file in tree_walk(url, auth=self.dcc_auth):
                yield file

    def process_url(self, url):
        """Process a url to a target file, allocating an id for it, inserting
        in the database, classifying it, and uploading it from the
        target dcc to our object store
        """
        # we first look for this file and skip if it's already there
        maybe_this_file = self.graph.nodes()\
                                    .labels("file")\
                                    .sysan({"source": "target_dcc", "url": url}).scalar()
        if maybe_this_file:
            self.log.info("Skipping file %s, since it was found in database as %s", url, maybe_this_file)
            return
        # the first thing we try to do is upload it to the object store,
        # since we need to get an md5sum before putting it in the database
        key = url.replace("https://target-data.nci.nih.gov/", "")
        self.log.info("requesting file %s from target dcc", key)
        resp = requests.get(url, auth=self.dcc_auth, stream=True)
        self.log.info("streaming %s from target into object store", key)
        stream = MD5SummingStream(resp.iter_content(1024 * 1024))
        self.container.upload_object_via_stream(stream, key)
        # sanity check on length
        assert int(resp.headers["content-length"]) == int(key.size)
        # ok, now we can allocate an id
        filename = url.split("/")[-1]
        doc = self.signpost.create()
        file_node = PsqlNode(
            node_id=doc.did,
            acl=self.acl,
            properties={
                "file_name": filename,
                "md5sum": stream.digest(),
                "file_size": self.determine_file_size(filename),
                "submitter_id": None,
                "state": "live",
                "state_comment": None,
            },
            system_annotations={
                "source": "target_dcc",
                "md5_source": "gdc_import_process",
                "url": url
            }
        )
        self.graph.node_insert(file_node)
        # TODO classify

    def sync(self):
        if self.pool:
            self.pool.map_async(self.process_url, self.file_links())
        else:
            for url in self.file_links():
                self.process_url(url)
