import os
from cdisutils.log import get_logger
from urlparse import urljoin
import requests
import re
import hashlib
from functools import partial
from lxml import html

from gdcdatamodel.models import File
from signpostclient import SignpostClient

from psqlgraph import PsqlGraphDriver

from zug.datamodel.target.classification import CLASSIFICATION
from zug.datamodel.target import PROJECTS

from zug.datamodel.tcga_dcc_sync import url_for

import libcloud.storage.drivers.s3
# upload in 500MB chunks
libcloud.storage.drivers.s3.CHUNK_SIZE = 500 * 1024 * 1024


CLASSIFICATION_ATTRS = ["data_subtype", "data_format", "platform",
                        "experimental_strategy", "tag"]


# TODO sigh, yet another copy paste job (from the api this time), this
# info should probably go in cdisutils
PROJECT_PHSID_MAPPING = {
    'ALL-P1': 'phs000463',
    'ALL-P2': 'phs000464',
    'AML': 'phs000465',
    'AML-IF': 'phs000515',
    'WT': 'phs000471',
    'CCSK': 'phs000466',
    'RT': 'phs000470',
    'NBL': 'phs000467',
    'OS': 'phs000468',
    'MDLS': 'phs000469',
}


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
        if not url.endswith("/"):
            url += "/"
        fulllink = urljoin(url, link.attrib["href"])
        if "/CGI/" in fulllink or "CBIIT" in fulllink:
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
    path = [d.lower() for d in path]
    try:
        potential_classifications = get_in(CLASSIFICATION, path)
    except KeyError:
        return None
    for regex, classification in potential_classifications.iteritems():
        if re.match(regex, filename, re.IGNORECASE):
            return classification


def process_url(kwargs, url):
    syncer = TARGETDCCFileSyncer(url, **kwargs)
    syncer.log.info("syncing file %s", url)
    syncer.sync()


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


class TARGETDCCEdgeBuilder(object):

    def __init__(self, file_node, graph, logger):
        self.file_node = file_node
        self.graph = graph
        self.log = logger

    def tie_file_to_attribute(self, file_node, attr, value):
        LABEL_MAP = {
            "platform": "generated_from",
            "data_subtype": "member_of",
            "data_format": "member_of",
            "tag": "member_of",
            "experimental_strategy": "member_of"
        }
        if not isinstance(value, list):
            # this is to handle the thing where tag is
            # sometimes a list and sometimes a string
            value = [value]
        for val in value:
            attr_node = self.graph.node_lookup_one(
                label=attr,
                property_matches={"name": val},
            )
            if not attr_node:
                self.log.error("attr_node with label %s and name %s not found (trying to tie for file %s) ", attr, val, file_node["file_name"])
            maybe_edge_to_attr_node = self.graph.edge_lookup_one(
                label=LABEL_MAP[attr],
                src_id=file_node.node_id,
                dst_id=attr_node.node_id
            )
            if not maybe_edge_to_attr_node:
                edge_to_attr_node = self.graph.get_PsqlEdge(
                    label=LABEL_MAP[attr],
                    src_id=file_node.node_id,
                    dst_id=attr_node.node_id,
                    src_label='file',
                    dst_label=attr,
                )
                self.graph.edge_insert(edge_to_attr_node)

    def build(self):
        self.classify()

    def classify(self):
        url = self.file_node.system_annotations["url"]
        project = url.split("/")[3]
        path = re.sub(".*\/{}\/((Discovery)|(Validation)|(Model_Systems))\/".format(project), "", url).split("/")
        self.log.info("classifying with path %s", path)
        classification = classify(path)
        self.log.info("classified as %s", classification)
        if not classification:
            self.log.warning("could not classify file %s", self.file_node)
            return
        for attr in CLASSIFICATION_ATTRS:
            if classification.get(attr):  # some don't have tags
                self.tie_file_to_attribute(self.file_node, attr, classification[attr])


class TARGETDCCProjectSyncer(object):

    def __init__(self, project, signpost_url=None,
                 graph_info=None, dcc_auth=None,
                 storage_info=None, pool=None):
        self.project = project
        self.dcc_auth = dcc_auth
        self.signpost_url = signpost_url
        self.graph_info = graph_info
        self.storage_info = storage_info
        self.base_url = "https://target-data.nci.nih.gov/{}/".format(project)
        self.pool = pool
        self.log = get_logger("taget_dcc_project_sync_" +
                              str(os.getpid()) +
                              "_" + self.project)

    def file_links(self):
        """A generator for links to files in this project"""
        for toplevel in ["Discovery", "Validation"]:
            url = urljoin(self.base_url, toplevel)
            for file in tree_walk(url, auth=self.dcc_auth):
                yield file

    def sync(self):
        self.log.info("running prelude")
        kwargs = {
            "signpost_url": self.signpost_url,
            "graph_info": self.graph_info,
            "storage_info": self.storage_info,
            "dcc_auth": self.dcc_auth,
        }
        if self.pool:
            self.log.info("syncing files using process pool")
            async_result = self.pool.map_async(partial(process_url, kwargs), self.file_links())
            async_result.get(999999)
        else:
            self.log.info("syncing files serially")
            for url in self.file_links():
                process_url(kwargs, url)


class TARGETDCCFileSyncer(object):

    def __init__(self, url, signpost_url=None,
                 graph_info=None, dcc_auth=None,
                 storage_info=None):
        assert url.startswith("https://target-data.nci.nih.gov")
        self.url = url
        self.project = url.split("/")[3]
        self.filename = self.url.split("/")[-1]
        assert self.project in PROJECTS
        self.signpost = SignpostClient(signpost_url, version="v0")
        self.graph = PsqlGraphDriver(graph_info["host"], graph_info["user"],
                                     graph_info["pass"], graph_info["database"])
        self.dcc_auth = dcc_auth
        self.storage_client = storage_info["driver"](storage_info["access_key"],
                                                     **storage_info["kwargs"])
        self.log = get_logger("taget_dcc_file_sync_" +
                              str(os.getpid()) +
                              "_" + self.filename)

    @property
    def acl(self):
        return ['phs000218', PROJECT_PHSID_MAPPING[self.project]]

    @property
    def container(self):
            return self.storage_client.get_container("target_dcc_protected")

    def sync(self):
        """Process a url to a target file, allocating an id for it, inserting
        in the database, classifying it, and uploading it from the
        target dcc to our object store
        """
        with self.graph.session_scope():
            # we first look for this file and skip if it's already there
            maybe_this_file = self.graph.nodes(File)\
                                        .sysan({"source": "target_dcc", "url": self.url}).scalar()
            if maybe_this_file:
                file_node = maybe_this_file
                self.log.info("Not downloading file %s, since it was found in database as %s", self.url, maybe_this_file)
            else:
                # the first thing we try to do is upload it to the object store,
                # since we need to get an md5sum before putting it in the database
                key = self.url.replace("https://target-data.nci.nih.gov/", "")
                self.log.info("requesting file %s from target dcc", key)
                resp = requests.get(self.url, auth=self.dcc_auth, stream=True)
                resp.raise_for_status()
                self.log.info("streaming %s from target into object store", key)
                stream = MD5SummingStream(resp.iter_content(1024 * 1024))
                obj = self.container.upload_object_via_stream(iter(stream), key)
                # sanity check on length
                assert int(resp.headers["content-length"]) == int(obj.size)
                # ok, now we can allocate an id
                self.log.info("allocating id for %s from signpost", key)
                doc = self.signpost.create(urls=[url_for(obj)])
                file_node = File(
                    node_id=doc.did,
                    acl=self.acl,
                    properties={
                        "file_name": self.filename,
                        "md5sum": stream.hexdigest(),
                        "file_size": int(obj.size),
                        "submitter_id": None,
                        "state": "live",
                        "state_comment": None,
                    },
                    system_annotations={
                        "source": "target_dcc",
                        "md5_source": "gdc_import_process",
                        "url": self.url
                    }
                )
                doc.refresh()
                assert doc.urls
                self.log.info("inserting %s in graph as %s", key, file_node)
                self.graph.node_insert(file_node)
            self.log.info("attempting to classify %s", file_node)
            builder = TARGETDCCEdgeBuilder(file_node, self.graph, self.log)
            try:
                builder.build()
            except Exception:
                self.log.exception("failed to classify %s", file_node)
