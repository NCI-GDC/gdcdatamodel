from cStringIO import StringIO
import tempfile
import tarfile
import re
import hashlib
from urlparse import urlparse, urljoin
from functools import partial
import copy
import os
import time
from contextlib import contextmanager

from lxml import html

import requests

from libcloud.storage.drivers.s3 import S3StorageDriver
from libcloud.storage.drivers.cloudfiles import OpenStackSwiftStorageDriver
from libcloud.storage.drivers.local import LocalStorageDriver

from psqlgraph import PsqlNode, PsqlEdge
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator

from gdcdatamodel import node_avsc_object, edge_avsc_object

from cdisutils.log import get_logger
from cdisutils.net import no_proxy

from zug.datamodel import tcga_classification

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
    def __init__(self, file_node, pg_driver):
        self.file_node = file_node
        self.pg_driver = pg_driver
        self.archive=self._get_archive()
        self.log = get_logger("tcga_dcc_edgebuilder_"+
                        str(os.getpid())+"_"+self.name)

    @property
    def name(self):
        return self.archive["archive_name"]

    def _get_archive(self):
        with self.pg_driver.session_scope():
            return self.pg_driver.nodes().labels('archive').with_edge_from_node('member_of',self.file_node).first().system_annotations

    def build(self):
        with self.pg_driver.session_scope() as session:
            self.classify(self.file_node, session)
            self.tie_file_to_center(self.file_node,session)

    def tie_file_to_center(self,file_node,session):
        query = self.pg_driver.nodes().labels('center').props({'center_type':self.archive['center_type'].upper(),'namespace':self.archive['center_name']})
        count = query.count()
        if count == 1:
            attr_node = query.first()
            maybe_edge_to_center = self.pg_driver.edge_lookup_one(
                label='submitted_by',
                src_id=file_node.node_id,
                dst_id=attr_node.node_id
            )
            if not maybe_edge_to_center:
                edge_to_attr_node = PsqlEdge(
                    label='submitted_by',
                    src_id=file_node.node_id,
                    dst_id=attr_node.node_id
                )
                self.pg_driver.edge_insert(edge_to_attr_node, session=session)

        elif count == 0:
            self.log.warning("center with type %s and namespace %s not found" ,
                                self.archive['center_type'],
                                self.archive['center_name'])
        else:
            self.log.warning("more than one center with type %s and namespace %s",
                                self.archive['center_type'],
                                self.archive['center_name'])


    def tie_file_to_atribute(self, file_node, attr, value, session):
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
            attr_node = self.pg_driver.node_lookup_one(
                label=attr,
                property_matches={"name": val},
                session=session
            )
            if not attr_node:
                self.log.error("attr_node with label %s and name %s not found (trying to tie for file %s) ", attr, val, file_node["file_name"])
            maybe_edge_to_attr_node = self.pg_driver.edge_lookup_one(
                label=LABEL_MAP[attr],
                src_id=file_node.node_id,
                dst_id=attr_node.node_id
            )
            if not maybe_edge_to_attr_node:
                edge_to_attr_node = PsqlEdge(
                    label=LABEL_MAP[attr],
                    src_id=file_node.node_id,
                    dst_id=attr_node.node_id
                )
                self.pg_driver.edge_insert(edge_to_attr_node, session=session)

    def classify(self, file_node, session):
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
                                          classification[attribute],
                                          session)
            else:
                self.log.warning("not tieing %s (node %s) to a %s",
                                 file_node["file_name"], file_node, attribute)
        return file_node

class TCGADCCArchiveSyncer(object):

    def __init__(self, archive, signpost=None,
                 pg_driver=None, dcc_auth=None,
                 scratch_dir=None, storage_client=None,
                 meta_only=False, force=False, no_upload=False,
                 max_memory=2*10**9):
        self.archive = archive
        self.signpost = signpost  # this should be SignpostClient object
        self.pg_driver = pg_driver
        self.pg_driver.node_validator = AvroNodeValidator(node_avsc_object)
        self.pg_driver.edge_validator = AvroEdgeValidator(edge_avsc_object)
        self.dcc_auth = dcc_auth
        self.storage_client = storage_client
        self.scratch_dir = scratch_dir
        self.meta_only = meta_only
        self.force = force
        self.max_memory = max_memory
        self.tarball = None
        self.no_upload = no_upload
        self.log = get_logger("tcga_dcc_sync_" +
                              str(os.getpid()) +
                              "_" + self.name)

    @property
    def name(self):
        return self.archive["archive_name"]

    def remove_old_versions(self, submitter_id, session):
        self.log.info("looking up old versions of archive %s in postgres", submitter_id)
        all_versions = self.pg_driver.node_lookup(
            label="archive",
            property_matches={"submitter_id": submitter_id},
            session=session
        ).all()
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
            for file in self.pg_driver.node_lookup(label="file", session=session)\
                                      .with_edge_to_node("member_of", old_archive)\
                                      .all():
                self.log.info("voiding file %s", str(file))
                self.pg_driver.node_delete(node=file, session=session)
            self.pg_driver.node_delete(node=old_archive, session=session)

    def sync_archive(self, session):
        # submitter_id is just the name without the revision or series
        # this will be identical between different versions of an
        # archive as new versions are submitted
        submitter_id = re.sub("\.(\d+?)\.(\d+)$", "", self.name)
        self.remove_old_versions(submitter_id, session)
        self.log.info("looking for archive %s in postgres", self.name)
        maybe_this_archive = self.pg_driver.node_lookup_one(
            label="archive",
            property_matches={"submitter_id": submitter_id,
                              "revision": self.archive["revision"]},
            session=session
        )
        if maybe_this_archive:
            node_id = maybe_this_archive.node_id
            self.log.info("found archive %s in postgres as node %s, not inserting", self.name, maybe_this_archive)
        else:
            node_id = self.signpost.create().did
            self.log.info("inserting new archive node in postgres with id: %s", node_id)
        sysan = self.archive
        sysan["source"] = "tcga_dcc"
        archive_node = self.pg_driver.node_merge(
            node_id=node_id,
            label='archive',
            acl=self.acl,
            properties={
                "submitter_id": submitter_id,
                "revision": self.archive["revision"]
            },
            system_annotations=sysan,
            session=session
        )
        project_node = self.pg_driver.node_lookup_one(
            label="project",
            property_matches={"name": self.archive["disease_code"]},
            session=session
        )
        maybe_edge_to_project = self.pg_driver.edge_lookup_one(
            src_id=archive_node.node_id,
            dst_id=project_node.node_id,
            label="member_of",
            session=session
        )
        if not maybe_edge_to_project:
            edge_to_project = PsqlEdge(
                src_id=archive_node.node_id,
                dst_id=project_node.node_id,
                label="member_of"
            )
            self.pg_driver.edge_insert(edge_to_project, session=session)
        return archive_node

    def lookup_file_in_pg(self, archive_node, filename, session):
        q = self.pg_driver.node_lookup(label="file",
                                       property_matches={"file_name": filename},
                                       session=session)\
                          .with_edge_to_node("member_of", archive_node)
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
        self.pg_driver.node_update(file_node, properties={"state": state})

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
        if not self.meta_only:
            tarinfo = self.tarball.getmember(self.full_name(filename))
            return int(tarinfo.size)
        else:
            return self.get_file_size_from_http(filename)

    def sync_file(self, filename, dcc_md5, session):
        """Sync this file in the database."""
        file_node = self.lookup_file_in_pg(self.archive_node, filename, session)
        md5, md5_source = self.determine_md5(filename, dcc_md5)
        if file_node:
            node_id = file_node.node_id
            self.log.info("file %s in already in postgres with id %s, not inserting", filename, node_id)
            file_node = self.pg_driver.node_update(
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
                session=session
            )
        else:
            node_id = self.signpost.create().did
            file_node = self.pg_driver.node_merge(
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
                session=session
            )
            self.log.info("inserting file %s into postgres with id %s", filename, node_id)
        maybe_edge_to_archive = self.pg_driver.edge_lookup_one(
            src_id=file_node.node_id,
            dst_id=self.archive_node.node_id,
            label="member_of",
            session=session
        )
        if not maybe_edge_to_archive:
            edge_to_archive = PsqlEdge(
                src_id=file_node.node_id,
                dst_id=self.archive_node.node_id,
                label="member_of"
            )
            self.pg_driver.edge_insert(edge_to_archive, session=session)
        edge_builder = TCGADCCEdgeBuilder(file_node, self.pg_driver)
        edge_builder.build()
        return file_node

    def get_manifest(self):
        if self.meta_only:
            resp = self.get_with_auth("/".join([self.archive["non_tar_url"],
                                                "MANIFEST.txt"]))
            manifest_data = resp.content
        else:
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
        if self.meta_only:
            NOT_PART_OF_ARCHIVE = ["Name", "Last modified",
                                   "Size", "Parent Directory"]
            resp = self.get_with_auth(self.archive["non_tar_url"])
            archives_html = html.fromstring(resp.content)
            return [elem.text for elem in archives_html.cssselect('a')
                    if elem.text not in NOT_PART_OF_ARCHIVE]
        else:
            # the reason for this is that sometimes the tarballs have
            # an useless entry that's just the name of the tarball, so we filter it out
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
        tries = 0
        while tries < 20:
            tries += 1
            self.log.info("downloading archive, try %s of 20", tries)
            resp = self.get_with_auth(self.archive["dcc_archive_url"], stream=True)
            if int(resp.headers["content-length"]) > self.max_memory:
                self.log.info("archive size is %s bytes, storing in "
                              "temp file on disk", resp.headers["content-length"])
                self.temp_file = tempfile.TemporaryFile(prefix=self.scratch_dir)
            else:
                self.log.info("archive size is %s bytes, storing in "
                              "memory in StringIO" , resp.headers["content-length"])
                self.temp_file = StringIO()
            for chunk in resp.iter_content(chunk_size=16000):
                self.temp_file.write(chunk)
            temp_file_len = self.temp_file.tell()
            if temp_file_len == int(resp.headers["content-length"]):
                self.temp_file.seek(0)
                self.log.info("archive downloaded, untaring")
                self.tarball = tarfile.open(fileobj=self.temp_file, mode="r:gz")
                return
            else:
                self.log.warning("archive download failed, got %s bytes but expected %s, retrying",
                                 temp_file_len, resp.headers["content-length"])
                self.temp_file.close()
        raise RuntimeError("failed to download archive with 20 retries")

    def manifest_is_complete(self, manifest, filenames):
        """Verify that the manifest is complete."""
        return all((name in manifest for name in filenames
                    if name != "MANIFEST.txt"))

    @property
    def container(self):
        if self.archive["protected"]:
            return self.storage_client.get_container("tcga_dcc_protected")
        else:
            return self.storage_client.get_container("tcga_dcc_public")

    def obj_for(self, url):
        # for now this assumes that the object can be found by self.storage_client
        parsed = urlparse(url)
        return self.storage_client.get_object(*parsed.path.split("/", 2)[1:])

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
            self.pg_driver.node_update(
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
        obj = self.upload_data(self.tarball.extractfile(name), name)
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
                    self.log.warning("%s caught, setting %s to %s", e, file, state)
                    self.set_file_state(file, state)
                    return
            self.log.exception("failure while trying to move %s from %s to %s via %s",
                               file, original_state, final_state, intermediate_state)
            self.set_file_state(file, original_state)
            raise

    def sync(self):
        if self.archive["disease_code"] == "FPPP":
            self.log.info("%s is an FPPP archive, skipping", self.name)
            return
        self.log.info("syncing archive %s", self.name)
        self.archive["non_tar_url"] = self.archive["dcc_archive_url"].replace(".tar.gz", "")
        self.acl = ["phs000178"] if self.archive["protected"] else ["open"]
        with self.pg_driver.session_scope() as session:
            self.archive_node = self.sync_archive(session)
            archive_doc = self.signpost.get(self.archive_node.node_id)
            if archive_doc and archive_doc.urls and not self.force:
                self.log.info("archive already has urls in signpost, "
                              "assuming it's complete")
                return
            if not self.meta_only:
                self.download_archive()
            manifest = self.get_manifest()
            filenames = self.get_files()
            file_nodes = []
            for filename in filenames:
                if filename != "MANIFEST.txt":
                    file_node = self.sync_file(filename, manifest.get(filename), session)
                    if file_node:
                        file_nodes.append(file_node)
        if not self.no_upload and not self.meta_only:
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

            # finally, upload the archive itself
            self.temp_file.seek(0)
            self.log.info("uploading archive to storage")
            obj = self.upload_data(self.temp_file, "/".join(["archives", self.name]))
            archive_url = url_for(obj)
            archive_doc = self.signpost.get(self.archive_node.node_id)
            if archive_doc and archive_doc.urls:
                self.log.info("archive already has signpost url")
            else:
                self.log.info("storing archive in signpost")
                doc = self.signpost.get(self.archive_node.node_id)
                doc.urls = [archive_url]
                doc.patch()
        else:
            self.log.info("skipping upload to object store")
        if not self.meta_only:
            self.temp_file.close()
