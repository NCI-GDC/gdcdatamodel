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
    # Since the changes to the TARGET site made a few links
    # that follow the image as well as the text, we have
    # to be more intelligent about what we're checking for.
    # The idea here is that we are seeing if the link mirrors
    # our current url. If so, we are assuming it's a link to
    # parent and avoiding adding so we don't recurse back up.
    links = [link for link in elem.cssselect("td a")
             if link.attrib["href"] not in url]
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
    return syncer.sync(), syncer.unclassified_files

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
            "tag": "memeber_of",
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
        project = url.split("/")[4]
        path = re.sub(".*\/{}\/((Discovery)|(Validation)|(Model_Systems))\/".format(project), "", url).split("/")
        self.log.info("classifying with path %s", path)
        classification = classify(path)
        self.log.info("classified as %s", classification)
        if not classification:
            self.log.warning("could not classify file %s", self.file_node['file_name'])
            return
        for attr in CLASSIFICATION_ATTRS:
            if classification.get(attr):  # some don't have tags
                self.tie_file_to_attribute(self.file_node, attr, classification[attr])


class TARGETDCCProjectSyncer(object):

    def __init__(self, project, signpost_url=None,
                 graph_info=None, dcc_auth=None,
                 storage_info=None, pool=None, bucket=None, verify_missing=True):
        self.project = project
        self.dcc_auth = dcc_auth
        self.signpost_url = signpost_url
        self.graph_info = graph_info
        self.storage_info = storage_info
        self.base_url = "https://target-data.nci.nih.gov/"
        self.pool = pool
        self.verify_missing = verify_missing
        self.log = get_logger("target_dcc_project_sync_" +
                              str(os.getpid()) +
                              "_" + self.project)
        self.graph = PsqlGraphDriver(graph_info["host"], graph_info["user"],
                                     graph_info["pass"], graph_info["database"])
        self.unclassified_files = 0

    def get_target_dcc_dict(self):
        """Create a dict of target_dcc files for current project"""
        target_dcc_files = {}
        with self.graph.session_scope():
            target_dcc_file_iter = self.graph.nodes(File).sysan(source="target_dcc").filter(
                File._sysan['url'].astext.contains("/%s/" % self.project)).all()
            for target_file in target_dcc_file_iter:
                if 'url' in target_file.sysan:
                    data = {}
                    data['delete'] = True
                    data['id'] = target_file.node_id
                    target_dcc_files[target_file.sysan['url']] = data

        return target_dcc_files

    def file_links(self):
        """A generator for links to files in this project"""

        # We're looking for a URL in the form
        # [base_url]/[Public/Controlled]/[Project Code]/[Discovery/Validation]
        # However, right now, "Validation is only present in the old base
        # url. Since there's a chance it could finally migrate, the code 
        # gracefully skips it if it 404s.
        for access_level in ["Public", "Controlled"]:
            url_part = access_level + "/" + self.project
            for toplevel in ["Discovery", "Validation"]:
                url_full = url_part + "/" + toplevel + "/"
                url = urljoin(self.base_url, url_full)
                self.log.info(url)
                resp = requests.head(url, auth=self.dcc_auth)
                if resp.status_code != 404:
                    return tree_walk(url, auth=self.dcc_auth)
                else:
                    self.log.warn("%s not present, skipping" % url)

    def check_if_missing(self, url, dcc_auth_info):
        if self.verify_missing:
            resp = requests.head(url, auth=dcc_auth_info)
        else:
            resp = requests.Response
            resp.status_code = 404

        if resp.status_code == 200:
            is_missing = False
        else:
            self.log.info("Status code: %d" % resp.status_code)
            is_missing = True

        return is_missing

    def cull(self, target_dcc_files, dcc_auth_info):
        """Set files to_delete to True based on dict passed"""
        with self.graph.session_scope():
            files_to_delete = 0
            for key, values in target_dcc_files.iteritems():
                if values['delete'] == True:
                    # try and get the file
                    print "Checking", key
                    if self.check_if_missing(key, dcc_auth_info):
                        files_to_delete += 1
                        if 'id' not in values:
                            self.log.warn("Warning, unable to delete %s, id missing." % key)
                        else:
                            node_to_delete = self.graph.nodes(File).get(values['id'])
                            node_to_delete.system_annotations['to_delete'] = True
                            self.log.info("Setting %s(%s) to be deleted" % (
                                node_to_delete.system_annotations['url'],
                                node_to_delete.node_id
                            ))
                            self.graph.current_session().merge(node_to_delete)
                    else:
                        self.log.warn("Warning, %s found, not deleting" % key)

            self.log.info("%d total files, %d marked for deletion" % (
                len(target_dcc_files), files_to_delete))

    def sync(self):
        self.log.info("running prelude")
        self.log.info("bucket is %s" % self.storage_info["bucket"])
        kwargs = {
            "signpost_url": self.signpost_url,
            "graph_info": self.graph_info,
            "storage_info": self.storage_info,
            "dcc_auth": self.dcc_auth,
        }

        file_dict = self.get_target_dcc_dict()

        if self.pool:
            self.log.info("syncing files using process pool")
            async_result, self.unclassified_files = self.pool.map_async(partial(process_url, kwargs), self.file_links())
            async_result.get(999999)
        else:
            self.log.info("syncing files serially")
            for url in self.file_links():
                node_id = process_url(kwargs, url)
                # mark url as found
                if url in file_dict:
                    file_dict[url]['delete'] = False
                else:
                    data = {}
                    data['delete'] = False
                    data['id'] = node_id
                    file_dict[url] = data

            self.cull(file_dict, self.dcc_auth)


class TARGETDCCFileSyncer(object):

    def __init__(self, url, signpost_url=None,
                 graph_info=None, dcc_auth=None,
                 storage_info=None, bucket=None):
        assert url.startswith("https://target-data.nci.nih.gov")
        self.url = url
        self.project = url.split("/")[4]
        self.filename = self.url.split("/")[-1]
        assert self.project in PROJECTS
        self.signpost = SignpostClient(signpost_url, version="v0")
        self.graph = PsqlGraphDriver(graph_info["host"], graph_info["user"],
                                     graph_info["pass"], graph_info["database"])
        self.dcc_auth = dcc_auth
        self.bucket = storage_info["bucket"]
        self.storage_client = storage_info["driver"](storage_info["access_key"],
                                                     **storage_info["kwargs"])
        self.log = get_logger("target_dcc_file_sync_" +
                              str(os.getpid()) +
                              "_" + self.filename)
        self.unclassified_files = 0

    @property
    def acl(self):
        # TODO: This will have to be intelligent
        return []

    @property
    def container(self):
            self.log.info(self.bucket)
            return self.storage_client.get_container(self.bucket)

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
                #self.log.info("Not downloading file %s, since it was found in database as %s", self.url, maybe_this_file)
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
            self.log.info("attempting to classify %s", file_node.properties['file_name'])
            builder = TARGETDCCEdgeBuilder(file_node, self.graph, self.log)
            try:
                builder.build()
            except Exception:
                self.unclassified_files += 1
                self.log.exception("failed to classify %s", file_node.properties['file_name'])
        return file_node.node_id
