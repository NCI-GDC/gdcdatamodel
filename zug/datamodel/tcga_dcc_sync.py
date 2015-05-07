from cStringIO import StringIO
import tempfile
import tarfile
import re
import hashlib
import random
from urlparse import urlparse, urljoin
from functools import partial
import copy
import os
import time
from contextlib import contextmanager

import requests

from libcloud.storage.drivers.s3 import S3StorageDriver
from libcloud.storage.drivers.cloudfiles import OpenStackSwiftStorageDriver
from libcloud.storage.drivers.local import LocalStorageDriver
from libcloud.common.types import LibcloudError

from gdcdatamodel import models
from gdcdatamodel.models import Archive, Center

from cdisutils.log import get_logger
from cdisutils.net import no_proxy

from zug.datamodel import tcga_classification
from zug.datamodel.latest_urls import LatestURLParser
# TODO put these somewhere that makes more sense
from zug.downloaders import StoppableThread, consul_heartbeat

from psqlgraph import PsqlGraphDriver
from signpostclient import SignpostClient

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

from consulate import Consul

S3 = get_driver(Provider.S3)


import libcloud.storage.drivers.s3
# upload in 500MB chunks
libcloud.storage.drivers.s3.CHUNK_SIZE = 500 * 1024 * 1024


def quickstats(graph):
    """
    This is just for using from the repl to get a quick sense of where we're at.
    """
    archives_in_dcc = list(LatestURLParser())
    names_in_dcc = {a["archive_name"] for a in archives_in_dcc}
    archive_nodes = graph.nodes(Archive).not_sysan({"to_delete": True}).all()
    names_in_graph = {n.system_annotations["archive_name"] for n in archive_nodes}
    removed = names_in_graph - names_in_dcc
    have = names_in_graph & names_in_dcc
    need = names_in_dcc - names_in_graph
    print "{} archives in graph and removed from dcc".format(len(removed))
    print "{} archives in graph and still in dcc".format(len(have))
    print "{} archives still to download from dcc".format(len(need))


def run_edge_build(g, files):
    """
    This is here just to be run from the REPL for one off jobs.
    """
    logger = get_logger("tcga_edge_build")
    logger.info("about to process %s nodes", len(files))
    for node in files:
        assert node.label == "file"
        assert node.system_annotations["source"] == "tcga_dcc"
        builder = TCGADCCEdgeBuilder(node, g, logger)
        logger.info("building edges for %s", node)
        builder.build()


class InvalidChecksumException(Exception):
    pass


def md5sum(iterable):
    md5 = hashlib.md5()
    for chunk in iterable:
        md5.update(chunk)
    return md5.hexdigest()


def iterable_from_file(fileobj, chunk_size=8192):
    return iter(partial(fileobj.read, chunk_size), '')


def fix_barcode(s):
    """Munge barcodes matched from filenames into correct format"""
    return s.replace("_", "-").upper()


def fix_uuid(s):
    """Munge uuids matched from filenames into correct format"""
    return s.replace("_", "-").lower()


def url_for(obj):
    """Return a url for a libcloud object."""
    DRIVER_TO_SCHEME = {
        S3StorageDriver: "s3",
        OpenStackSwiftStorageDriver: "swift",
        LocalStorageDriver: "file"
    }
    scheme = DRIVER_TO_SCHEME[obj.driver.__class__]
    host = obj.driver.connection.host
    container = obj.container.name
    name = obj.name
    url = "{scheme}://{host}/{container}/{name}".format(scheme=scheme,
                                                        host=host,
                                                        container=container,
                                                        name=name)
    return url

CLASSIFICATION_ATTRS = ["data_subtype", "data_format", "platform",
                        "experimental_strategy", "tag"]


def classify(archive, filename):
    """given a filename and an archive that it came from, attempt to
    classify it. return a dictionary representing the
    classification.
    """
    if archive["data_level"] == "mage-tab":
        return {"data_format": "to_be_ignored"}  # no classification for mage-tabs
    data_type = archive["data_type_in_url"]
    data_level = str(archive["data_level"])
    platform = archive["platform"]
    potential_classifications = tcga_classification[data_type][data_level][platform]
    for possibility in potential_classifications:
        match = re.match(possibility["pattern"], filename)
        if match:
            result = copy.deepcopy(possibility["category"])
            # if the classification doesn't have a platform
            if not result.get("platform"):
                result["platform"] = platform
            result["data_format"] = possibility["data_format"]
            if possibility.get("captured_fields"):
                for i, field in enumerate(possibility["captured_fields"]):
                    if field not in ['_', '-']:
                        if field.endswith("barcode"):
                            result[field] = fix_barcode(match.groups()[i])
                        elif field.endswith("uuid"):
                            result[field] = fix_uuid(match.groups()[i])
                        else:
                            result[field] = match.groups()[i]
            return result
    raise RuntimeError("file {}/{} failed to classify".format(archive["archive_name"], filename))


class TCGADCCEdgeBuilder(object):

    def __init__(self, file_node, graph, logger):
        self.file_node = file_node
        self.graph = graph
        self.archive = self._get_archive()
        self.log = logger

    @property
    def name(self):
        return self.archive["archive_name"]

    def _get_archive(self):
        with self.graph.session_scope():
            return self.graph.nodes(Archive)\
                             .with_edge_from_node(
                                 models.FileMemberOfArchive,
                                 self.file_node)\
                             .first()\
                             .system_annotations

    def build(self):
        with self.graph.session_scope():
            self.classify(self.file_node)
            self.tie_file_to_center(self.file_node)

    def tie_file_to_center(self, file_node):
        center_type = self.archive['center_type']
        namespace = self.archive['center_name']
        if namespace == 'mdanderson.org' and center_type.upper() == 'CGCC':
            query = self.graph.nodes(Center).props({'code':'20'})
        elif namespace == 'genome.wustl.edu' and center_type.upper() == 'CGCC':
            query = self.graph.nodes(Center).props({'code':'21'})
        elif namespace == 'bcgsc.ca' and center_type.upper() == 'CGCC':
            query = self.graph.nodes(Center).props({'code':'13'})
        else:
            query = self.graph.nodes(Center).props({'center_type':self.archive['center_type'].upper(),'namespace':self.archive['center_name']})

        count = query.count()
        if count == 1:
            attr_node = query.first()
            edge_to_center = models.FileSubmittedByCenter(
                src_id=file_node.node_id,
                dst_id=attr_node.node_id,
            )
            self.graph.current_session().merge(edge_to_center)
        elif count == 0:
            self.log.warning("center with type %s and namespace %s not found",
                             self.archive['center_type'],
                             self.archive['center_name'])
        else:
            self.log.warning("more than one center with type %s and namespace %s",
                             self.archive['center_type'],
                             self.archive['center_name'])

    def tie_file_to_atribute(self, file_node, attr, value):
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
            edge_to_attr_node = self.graph.get_PsqlEdge(
                label=LABEL_MAP[attr],
                src_id=file_node.node_id,
                dst_id=attr_node.node_id,
                src_label='file',
                dst_label=attr,
            )
            self.graph.current_session().merge(edge_to_attr_node)

    def classify(self, file_node):
        classification = classify(self.archive, file_node["file_name"])
        if classification["data_format"] == "to_be_ignored":
            self.log.info("ignoring %s for classification",
                          file_node["file_name"])
            return file_node
        # TODO this is a lot of edge cases, needs to be simplified
        if (not classification or "to_be_determined" in classification.values()
            or "to_be_determined_protein_exp" in classification.values()
            or classification == {"data_format": "TXT"}):
            self.log.error("file %s classified as %s, marking",
                           file_node["file_name"], classification)
            file_node.system_annotations["unclassified"] = True
            return file_node
        for k, v in classification.iteritems():
            if k.startswith("_"):
                file_node.system_annotations[k] = v
        # we need to create edges to: data_subtype, data_format,
        # platform, experimental_strategy, tag.
        #
        # TODO drop any existing classification?
        for attribute in CLASSIFICATION_ATTRS:
            if classification.get(attribute):
                self.tie_file_to_atribute(file_node, attribute,
                                          classification[attribute])
            else:
                self.log.warning("not tieing %s (node %s) to a %s",
                                 file_node["file_name"], file_node, attribute)
        return file_node


class TCGADCCArchiveSyncer(object):

    def __init__(self, archive_id=None, max_memory=2*10**9,
                 s3=None, consul_prefix="tcgadccsync"):
        self.graph = PsqlGraphDriver(
            os.environ["PG_HOST"],
            os.environ["PG_USER"],
            os.environ["PG_PASS"],
            os.environ["PG_NAME"],
        )
        self.signpost = SignpostClient(os.environ["SIGNPOST_URL"])
        self.consul = Consul()
        self.dcc_auth = (os.environ["DCC_USER"], os.environ["DCC_PASS"])
        if not s3:
            self.s3 = S3(
                os.environ["S3_ACCESS_KEY"],
                os.environ["S3_SECRET_KEY"],
                host=os.environ["S3_HOST"],
                secure=False,
            )
        else:
            self.s3 = s3
        self.scratch_dir = os.environ["SCRATCH_DIR"]
        self.protected_bucket = os.environ["TCGA_PROTECTED_BUCKET"]
        self.public_bucket = os.environ["TCGA_PUBLIC_BUCKET"]
        self.archive_id = archive_id
        self.archive_node = None  # this gets filled in later
        self.max_memory = max_memory
        # these two also get filled in later
        self.temp_file = None
        self.tarball = None
        self.consul_prefix = consul_prefix
        self.log = get_logger("tcga_dcc_sync_" +
                              str(os.getpid()))

    # TODO this is largely a copy paste job from
    # downloaders.py, would like to clean up at some point

    def start_consul_session(self):
        self.log.info("Starting new consul session")
        self.consul_session = self.consul.session.create(
            behavior="delete",
            ttl="60s",
        )
        self.log.info("Consul session %s started, forking thread to heartbeat", self.consul_session)
        self.heartbeat_thread = StoppableThread(target=consul_heartbeat,
                                                args=(self.consul_session, 10))
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    @property
    def name(self):
        return self.archive["archive_name"]

    def delete_later(self, node):
        """
        Mark a node for deletion. The reason we do this instead of just
        deleting immediately is so that the elasticsearch index
        generation code has time to run again and remove this data
        from the index (so that it doesn't 404 when people try to
        download it).
        """
        self.log.info("Marking %s as to_delete in system annotations", node)
        self.graph.node_update(node, system_annotations={"to_delete": True})

    def remove_old_versions(self, submitter_id):
        self.log.info("looking up old versions of archive %s in postgres", submitter_id)
        all_versions = self.graph.nodes(Archive)\
                                 .props({"submitter_id": submitter_id})\
                                 .not_sysan({"to_delete": True})\
                                 .all()
        old_versions = [version for version in all_versions
                        if version["revision"] < self.archive["revision"]]
        if len(old_versions) > 1:
            # since we void all old versions of an archive when we add a new one,
            # there should never be more than one old version in the database
            raise ValueError("multiple old versions of archive {} found".format(submitter_id))
        if old_versions:
            old_archive = old_versions[0]
            self.log.info("old revision (%s) of archive %s found, voiding it and associated files",
                          old_archive["revision"],
                          submitter_id)
            for file in self.graph.node_lookup(label="file")\
                                  .with_edge_to_node(
                                      models.FileMemberOfArchive,
                                      old_archive).all():
                self.delete_later(file)
            self.delete_later(old_archive)

    def sync_archive(self):
        # submitter_id is just the name without the revision or series
        # this will be identical between different versions of an
        # archive as new versions are submitted
        submitter_id = re.sub("\.(\d+?)\.(\d+)$", "", self.name)
        self.remove_old_versions(submitter_id)
        self.log.info("looking for archive %s in postgres", self.name)
        maybe_this_archive = self.graph.node_lookup_one(
            label="archive",
            property_matches={"submitter_id": submitter_id,
                              "revision": self.archive["revision"]},
        )
        if maybe_this_archive:
            node_id = maybe_this_archive.node_id
            self.log.info("found archive %s in postgres as node %s, not inserting", self.name, maybe_this_archive)
        else:
            node_id = self.signpost.create().did
            self.log.info("inserting new archive node in postgres with id: %s", node_id)
        sysan = self.archive
        sysan["source"] = "tcga_dcc"
        archive_node = self.graph.node_merge(
            node_id=node_id,
            label='archive',
            acl=self.acl,
            properties={
                "submitter_id": submitter_id,
                "revision": self.archive["revision"]
            },
            system_annotations=sysan,
        )
        project_node = self.graph.node_lookup_one(
            label="project",
            property_matches={"code": self.archive["disease_code"]},
        )
        edge_to_project = models.ArchiveMemberOfProject(
            src_id=archive_node.node_id,
            dst_id=project_node.node_id,
        )
        self.graph.current_session().merge(edge_to_project)
        return archive_node

    def lookup_file_in_pg(self, archive_node, filename):
        q = self.graph.node_lookup(
            label="file",
            property_matches={
                "file_name": filename
            }).with_edge_to_node(models.FileMemberOfArchive, archive_node)
        file_nodes = q.all()
        if not file_nodes:
            return None
        if len(file_nodes) > 1:
            raise ValueError("multiple files with the same name found in archive {}".format(archive_node))
        else:
            return file_nodes[0]

    def get_file_size_from_http(self, filename):
        base_url = self.archive["dcc_archive_url"].replace(".tar.gz", "/")
        file_url = urljoin(base_url, filename)
        # it's necessary to specify the accept-encoding here so that
        # there server doesn't send us gzipped content and we get the
        # wrong length
        resp = self.get_with_auth(file_url, stream=True, headers={"accept-encoding": "text/plain"})
        return int(resp.headers["content-length"])

    def set_file_state(self, file_node, state):
        self.graph.node_update(file_node, properties={"state": state})

    def extract_file_data(self, filename):
        return self.tarball.extractfile("/".join([self.name, filename]))

    def determine_md5(self, filename, dcc_md5):
        """If the md5 from the dcc is None, we need to compute our own, so do
        that here."""
        if dcc_md5:
            return dcc_md5, "tcga_dcc"
        else:
            if not self.tarball:
                # now we have no choice in order to get the correct md5
                self.download_archive()
            md5 = md5sum(iterable_from_file(
                self.extract_file_data(filename)))
            return md5, "gdc_import_process"

    def determine_file_size(self, filename):
        tarinfo = self.tarball.getmember(self.full_name(filename))
        return int(tarinfo.size)

    def sync_file(self, filename, dcc_md5):
        """Sync this file in the database."""
        file_node = self.lookup_file_in_pg(self.archive_node, filename)
        md5, md5_source = self.determine_md5(filename, dcc_md5)
        if file_node:
            node_id = file_node.node_id
            self.log.info("file %s in already in postgres with id %s, not inserting", filename, node_id)
            self.graph.node_update(
                file_node,
                acl=self.acl,
                properties={
                    "file_name": filename,
                    "md5sum": md5,
                    "file_size": self.determine_file_size(filename),
                    "submitter_id": None,
                },
                system_annotations={
                    "source": "tcga_dcc",
                    "md5_source": md5_source,
                },
            )
        else:
            node_id = self.signpost.create().did
            file_node = self.graph.node_merge(
                node_id=node_id,
                label="file",
                acl=self.acl,
                properties={
                    "file_name": filename,
                    "md5sum": md5,
                    "file_size": self.determine_file_size(filename),
                    "state": "submitted",
                    "state_comment": None,
                    "submitter_id": None,
                },
                system_annotations={
                    "source": "tcga_dcc",
                    "md5_source": md5_source,
                },
            )
            self.log.info("inserting file %s into postgres with id %s", filename, node_id)
        edge_to_archive = models.FileMemberOfArchive(
            src_id=file_node.node_id,
            dst_id=self.archive_node.node_id,
        )
        self.graph.current_session().merge(edge_to_archive)
        edge_builder = TCGADCCEdgeBuilder(file_node, self.graph, self.log)
        edge_builder.build()
        return file_node

    def get_manifest(self):
        manifest_tarinfo = self.tarball.getmember("{}/MANIFEST.txt".format(self.name))
        manifest_data = self.tarball.extractfile(manifest_tarinfo).read()
        res = {}
        try:
            for line in manifest_data.splitlines():
                md5, filename = line.split()
                res[filename] = md5
        except ValueError:
            self.log.warning("manifest does not have checksums")
            return {}
        return res

    def get_files(self):
        # the reason for this is that sometimes the tarballs have
        # a useless entry that's just the name of the tarball, so we filter it out
        names = [name for name in self.tarball.getnames() if name != self.name]
        return [name.replace(self.name + "/", "") for name in names]

    def get_with_auth(self, url, **kwargs):
        tries = 0
        while tries < 120:
            try:
                resp = requests.get(url, auth=self.dcc_auth,
                                    allow_redirects=False, **kwargs)
                redirects = 0
                while resp.is_redirect and redirects < 5:
                    # sometimes it redirects, try again. normally requests
                    # does this automatically, but this doesn't work with auth
                    redirects += 1
                    resp = requests.get(resp.headers["location"], auth=self.dcc_auth,
                                        allow_redirects=False, **kwargs)
                # ENTERING GROSS HACK ZONE
                # (+1 for informative header)
                #
                # sometimes it just returns a 401 (no redirect) when you're
                # trying to hit tcga-data but you want
                # tcga-data-secure. somehow Chrome manages to figure this out,
                # I think it has something to do with cookies. In any case I
                # do it manually here.
                if resp.status_code == 401:
                    fixed_url = re.sub("tcga-data", "tcga-data-secure", url)
                    resp = requests.get(fixed_url, auth=self.dcc_auth,
                                        allow_redirects=False, **kwargs)
                # EXITING GROSS HACK ZONE
                return resp
            except requests.ConnectionError:
                tries += 1
                time.sleep(1)
                self.log.exception("caught ConnectionError talking to %s, retrying", url)
        raise RuntimeError("retries exceeded on {}".format(url))

    def download_archive(self):
        self.log.info("downloading archive")
        info_resp = self.get_with_auth(self.archive["dcc_archive_url"], stream=True)
        content_length = int(info_resp.headers["content-length"])
        info_resp.close()  # to make sure we free the connection
        if content_length > self.max_memory:
            self.log.info("archive size is %s bytes, storing in "
                          "temp file on disk", content_length)
            self.temp_file = tempfile.TemporaryFile(prefix=self.scratch_dir)
        else:
            self.log.info("archive size is %s bytes, storing in "
                          "memory in StringIO", content_length)
            self.temp_file = StringIO()
        while self.temp_file.tell() != content_length:
            self.log.info("sending new download request. "
                          "downloaded so far: %s / %s bytes, %s percent complete",
                          self.temp_file.tell(), content_length,
                          float(self.temp_file.tell()) / float(content_length))
            range = "bytes={start}-{end}".format(start=self.temp_file.tell(),
                                                 end=content_length)
            self.log.info("requesting %s", range)
            resp = self.get_with_auth(self.archive["dcc_archive_url"],
                                      headers={"Range": range}, stream=True)
            resp.raise_for_status()
            self.log.info("writing chunks to temp file")
            for chunk in resp.iter_content(chunk_size=10000000):
                self.temp_file.write(chunk)
        temp_file_len = self.temp_file.tell()
        if temp_file_len == content_length:
            self.temp_file.seek(0)
            self.log.info("archive downloaded, untaring")
            self.tarball = tarfile.open(fileobj=self.temp_file, mode="r:gz")
            return
        else:
            self.log.error("archive download failed, got %s bytes but expected %s, something is wrong",
                           temp_file_len, content_length)
            self.temp_file.close()

    def manifest_is_complete(self, manifest, filenames):
        """Verify that the manifest is complete."""
        return all((name in manifest for name in filenames
                    if name != "MANIFEST.txt"))

    @property
    def container(self):
        if self.archive["protected"]:
            return self.s3.get_container(self.protected_bucket)
        else:
            return self.s3.get_container(self.public_bucket)

    def obj_for(self, url):
        # for now this assumes that the object can be found by self.s3
        parsed = urlparse(url)
        return self.s3.get_object(*parsed.path.split("/", 2)[1:])

    @no_proxy()
    def upload_data(self, fileobj, key):
        obj = self.container.upload_object_via_stream(iterable_from_file(fileobj),
                                                      key)
        return obj

    def full_name(self, filename):
        return "/".join([self.name, filename])

    def verify(self, node):
        doc = self.signpost.get(node.node_id)
        urls = doc.urls
        if not urls:
            raise RuntimeError("no urls in signpost for file {}, node: {}".format(node.file_name, node))
        obj = self.obj_for(urls[0])
        expected_sum = node["md5sum"]
        actual_sum = md5sum(obj.as_stream())
        if actual_sum != expected_sum:
            self.graph.node_update(
                node, properties={"state_comment": "bad md5sum"})
            self.log.warning("file %s has invalid checksum", node["file_name"])
            raise InvalidChecksumException()

    def upload(self, node):
        doc = self.signpost.get(node.node_id)
        urls = doc.urls
        if urls:
            self.log.info("file %s already in signpost, skipping",
                          node["file_name"])
            return
        name = self.full_name(node["file_name"])
        tries = 0
        while True:
            tries += 1
            try:
                obj = self.upload_data(self.tarball.extractfile(name), name)
                break
            except LibcloudError as e:
                if tries > 10:
                    self.log.error("couldn't upload in 10 tries, failing")
                    raise e
                else:
                    self.log.warning("caught %s while trying to upload, retrying", e)
                    time.sleep(3)
        new_url = url_for(obj)
        doc.urls = [new_url]
        doc.patch()

    @contextmanager
    def state_transition(self, file, intermediate_state, final_state,
                         error_states={}):
        """Try to do something to a file node, setting it's state to
        intermediate_state while the thing is being done, moving to
        final_state if the thing completes successfully, falling back to the original
        state if the thing fails
        """
        original_state = file["state"]
        try:
            self.set_file_state(file, intermediate_state)
            yield
            self.set_file_state(file, final_state)
        except BaseException as e:
            for err_cls, state in error_states.iteritems():
                if isinstance(e, err_cls):
                    self.log.warning("%s caught, setting %s to %s", err_cls.__name__, file, state)
                    self.set_file_state(file, state)
                    return
            self.log.exception("failure while trying to move %s from %s to %s via %s",
                               file, original_state, final_state, intermediate_state)
            self.set_file_state(file, original_state)
            raise

    def get_consul_lock(self, archive):
        key = "{}/current/{}".format(self.consul_prefix, archive["archive_name"])
        self.log.info("Attempting to lock %s in consul", key)
        return self.consul.kv.acquire_lock(key, self.consul_session)

    def list_locked_archives(self):
        current = [key.split("/")[-1] for key in
                   self.consul.kv.find("/".join([self.consul_prefix, "current"]))]
        self.log.info("there are %s archives currently being synced: %s", len(current), current)
        return current


    def get_archive(self):
        """
        Our strategy for choosing an archive to work on is if we have
        self.archive_id just use that one. Otherwise:

        1) pull all archives from database and latest list from the dcc
        2) if we have any unuploaded archives in the database (as indicated by system_annotations["uploaded"]),
           work on one of those at random
        3) otherwise, choose one at random from the dcc that we don't yet have in the database

        """
        self.log.info("Fetching latest archives list from DCC")
        archives = [a for a in LatestURLParser() if a["disease_code"] != "FPPP"]
        if self.archive_id:
            with self.graph.session_scope():
                self.log.info("Archive with id %s requested, finding in database", self.archive_id)
                archive_node = self.graph.nodes(Archive).ids(self.archive_id).one()
                assert archive_node.label == "archive"
                self.log.info("Finding matching archive from DCC")
                archive = [archive for archive in archives
                           if archive["archive_name"] == archive_node.system_annotations["archive_name"]][0]
                if not self.get_consul_lock(archive):
                    msg = "Couldn't lock archive {} for requested id {}".format(
                        archive["archive_name"],
                        self.archive_id
                    )
                    raise RuntimeError(msg)
                else:
                    self.archive_node = archive_node
                    self.archive = archive
                    return self.archive
        tries = 0
        while tries < 5:
            tries += 1
            self.log.info("Finding archive to work on, try %s", tries)
            with self.graph.session_scope():
                self.log.info("Fetching archive nodes from database")
                archive_nodes = self.graph.nodes(Archive).all()
                current = self.list_locked_archives()
                names_in_dcc = [archive["archive_name"] for archive in archives]
                unuploaded = [node for node in archive_nodes
                              if not node.system_annotations.get("uploaded")
                              # this line filters nodes that are already locked
                              and node.system_annotations["archive_name"] not in current
                              # this line filters nodes that have been removed from the dcc
                              and node.system_annotations["archive_name"] in names_in_dcc]
                if unuploaded:
                    self.log.info("There are %s unuploaded archive nodes in the db, choosing one of them", len(unuploaded))
                    random.shuffle(unuploaded)
                    # there are unuploaded nodes
                    choice = unuploaded[0]
                    archive = [archive for archive in archives
                               if archive["archive_name"] == choice.system_annotations["archive_name"]][0]
                    if not self.get_consul_lock(archive):
                        self.log.warning("Couldn't acquire consul lock on %s, retrying", archive["archive_name"])
                        continue
                    self.archive = archive
                    self.archive_node = choice
                else:
                    self.log.info("Found no unuploaded nodes in the database, choosing new archive from DCC")
                    names_in_db = [node.system_annotations["archive_name"]
                                   for node in archive_nodes]
                    unimported = [archive for archive in archives
                                  if archive["archive_name"] not in names_in_db + current]
                    if not unimported:
                        self.log.info("No unuploaded nodes or unimported archives found, we're all caught up!")
                        return None
                    random.shuffle(unimported)
                    archive = unimported[0]
                    if not self.get_consul_lock(archive):
                        self.log.warning("Couldn't acquire consul lock on %s, retrying", archive["archive_name"])
                        continue
                    self.archive = unimported[0]
                self.log.info("chose archive %s to work on", self.name)
                return self.archive
        raise RuntimeError("Couldn't lock archive to download in five tries")

    def upload_archive(self):
        self.temp_file.seek(0)
        self.log.info("uploading archive to storage")
        obj = self.upload_data(self.temp_file, "/".join(["archives", self.name]))
        archive_url = url_for(obj)
        archive_doc = self.signpost.get(self.archive_node.node_id)
        if archive_doc and archive_doc.urls:
            self.log.info("archive already has signpost url")
        else:
            self.log.info("storing archive url %s in signpost", archive_url)
            doc = self.signpost.get(self.archive_node.node_id)
            doc.urls = [archive_url]
            doc.patch()
            self.log.info("noting that archive node %s is uploaded", self.archive_node)
            self.graph.node_update(
                self.archive_node,
                system_annotations={
                    "uploaded": True
                }
            )

    def cleanup(self):
        self.log.info("Cleaning up")
        if self.temp_file:
            self.log.info("Closing temp file in which archive was stored")
            self.temp_file.close()
        self.log.info("Stopping consul heartbeat thread")
        self.heartbeat_thread.stop()
        self.log.info("Waiting to join heartbeat thread . . .")
        self.heartbeat_thread.join(20)
        if self.heartbeat_thread.is_alive():
            self.log.warning("Joining heartbeat thread failed after 20 seconds!")
        self.log.info("Invalidating consul session")
        self.consul.session.destroy(self.consul_session)

    def transition_files_to_live(self, file_nodes):
        for node in file_nodes:
            if node["state"] in ["submitted", "uploading"]:
                with self.state_transition(node, "uploading", "uploaded"):
                    self.log.info("uploading file %s (%s)", node, node["file_name"])
                    self.upload(node)
            if node["state"] in ["uploaded", "validating"]:
                with self.state_transition(node, "validating", "live",
                                           error_states={InvalidChecksumException: "invalid"}):
                    self.log.info("validating file %s (%s)", node, node["file_name"])
                    self.verify(node)
            if node["state"] in ["live", "invalid"]:
                self.log.info("%s (%s) is in state %s",
                              node, node["file_name"], node["state"])

    def download_archive_and_sync_files(self):
        with self.graph.session_scope():
            self.archive_node = self.sync_archive()
            self.download_archive()
            manifest = self.get_manifest()
            filenames = self.get_files()
            file_nodes = []
            for filename in filenames:
                if filename != "MANIFEST.txt":
                    file_node = self.sync_file(filename, manifest.get(filename))
                    if file_node:
                        file_nodes.append(file_node)
            return file_nodes

    def sync(self):
        self.start_consul_session()
        # this sets self.archive and potentially self.archive_node
        if not self.get_archive():
            # if this returns None, it means we're all done
            return
        self.log.info("syncing archive %s", self.name)
        self.archive["non_tar_url"] = self.archive["dcc_archive_url"].replace(".tar.gz", "")
        self.acl = ["phs000178"] if self.archive["protected"] else ["open"]
        try:
            file_nodes = self.download_archive_and_sync_files()
            self.transition_files_to_live(file_nodes)
            # finally, upload the archive itself
            self.upload_archive()
            self.temp_file.close()
        finally:
            self.cleanup()
