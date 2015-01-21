from cStringIO import StringIO
import tempfile
import tarfile
import re
import hashlib
from urlparse import urlparse
from functools import partial
import copy
import os

from lxml import html

import requests

from libcloud.storage.drivers.s3 import S3StorageDriver
from libcloud.storage.drivers.cloudfiles import OpenStackSwiftStorageDriver
from libcloud.storage.drivers.local import LocalStorageDriver

from psqlgraph import PsqlNode, PsqlEdge
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator

from gdcdatamodel import node_avsc_object, edge_avsc_object

from cdisutils.log import get_logger

from zug.datamodel import tcga_classification


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
    """Given a filename and an archive that it came from, attempt to
    classify it. Return a dictionary representing the
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


class TCGADCCArchiveSyncer(object):

    MAX_BYTES_IN_MEMORY = 2 * (10**9)  # 2GB TODO make this configurable
    SIGNPOST_VERSION = "v0"

    def __init__(self, archive, signpost_url=None,
                 pg_driver=None, dcc_auth=None,
                 scratch_dir=None, storage_client=None,
                 dryrun=False):
        self.archive = archive
        self.signpost_url = signpost_url
        self.pg_driver = pg_driver
        self.pg_driver.node_validator = AvroNodeValidator(node_avsc_object)
        self.pg_driver.edge_validator = AvroEdgeValidator(edge_avsc_object)
        self.dcc_auth = dcc_auth
        self.storage_client = storage_client
        self.scratch_dir = scratch_dir
        self.dryrun = dryrun
        self.log = get_logger("tcga_dcc_sync_" +
                              str(os.getpid()) +
                              "_" + self.name)

    @property
    def name(self):
        return self.archive["archive_name"]

    def store_archive_in_pg(self, session):
        # submitter_id is just the name without the revision or series
        # this will be identical between different versions of an
        # archive as new versions are submitted
        submitter_id = re.sub("\.(\d+?)\.(\d+)$", "", self.name)
        self.log.info("looking for archive %s in postgres", self.name)
        maybe_this_archive = self.pg_driver.node_lookup_one(
            label="archive",
            property_matches={"submitter_id": submitter_id,
                              "revision": self.archive["revision"]},
            session=session
        )
        if maybe_this_archive:
            self.log.info("found archive %s in postgres, not inserting", self.name)
            return maybe_this_archive
        self.log.info("looking up old versions of archive %s in postgres", submitter_id)
        old_versions = self.pg_driver.node_lookup(
            label="archive",
            property_matches={"submitter_id": submitter_id},
            session=session
        ).all()
        if len(old_versions) > 1:
            # since we void all old versions of an archive when we add a new one,
            # there should never be more than one old version in the database
            raise ValueError("multiple old versions of archive {} found".format(submitter_id))
        if old_versions:
            old_archive = old_versions[0]
            self.log.info("old revision (%s) of archive %s found, voiding it and associated files",
                          old_archive.properties["revision"],
                          submitter_id)
            # TODO it would be awesome to verify that the changes we make actually match what's in
            # CHANGES_DCC.txt,
            # first get all the files related to this archive and void them
            for file in self.pg_driver.node_lookup(label="file", session=session)\
                                      .with_edge_to_node("member_of", old_archive)\
                                      .all():
                self.log.info("voiding file %s", str(file))
                self.pg_driver.node_delete(node=file, session=session)
            self.pg_driver.node_delete(node=old_archive, session=session)
        new_archive_node = PsqlNode(
            node_id=self.allocate_id_from_signpost(),
            label="archive",
            properties={"submitter_id": submitter_id,
                        "revision": self.archive["revision"]})
        project_node = self.pg_driver.node_lookup_one(label="project",
                                                      property_matches={"name": self.archive["disease_code"]},
                                                      session=session)
        edge_to_project = PsqlEdge(src_id=new_archive_node.node_id,
                                   dst_id=project_node.node_id,
                                   label="member_of")
        if self.dryrun:
            self.log.info("dryrun mode, inserting new archive node in postgres: %s", str(new_archive_node))
            return new_archive_node
        self.log.info("inserting new archive node in postgres: %s", str(new_archive_node))
        self.pg_driver.node_insert(new_archive_node, session=session)
        self.pg_driver.edge_insert(edge_to_project, session=session)
        return new_archive_node

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

    def allocate_id_from_signpost(self):
        """Retrieve a new empty did from signpost."""
        resp = requests.post("/".join([self.signpost_url,
                                       self.SIGNPOST_VERSION,
                                       "did"]),
                             json={"urls": []})
        resp.raise_for_status()
        return resp.json()["did"]

    def get_urls_from_signpost(self, did):
        """Retrieve all the urls associated with a did in signpost."""
        resp = requests.get("/".join([self.signpost_url,
                                      self.SIGNPOST_VERSION,
                                      "did",
                                      did]))
        resp.raise_for_status()
        return resp.json()["urls"]

    def store_url_in_signpost(self, did, url):
        # replace whatever urls are in there with the one passed in
        # going to have to go a GET first to get the rev
        getresp = requests.get("/".join([self.signpost_url,
                                         self.SIGNPOST_VERSION,
                                         "did",
                                         did]))
        getresp.raise_for_status()
        getjson = getresp.json()
        rev = getjson["rev"]
        old_urls = getjson["urls"]
        if old_urls:
            raise RuntimeError("attempt to replace existing urls on did {}".format(did))
        patchresp = requests.patch("/".join([self.signpost_url,
                                             self.SIGNPOST_VERSION,
                                             "did",
                                             did]),
                                   json={"urls": [url], "rev": rev})
        patchresp.raise_for_status()

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
            attr_node = self.pg_driver.node_lookup_one(label=attr,
                                                       property_matches={"name": val},
                                                       session=session)
            if not attr_node:
                self.log.error("attr_node with label %s and name %s not found (trying to tie for file %s) ", attr, val, file_node["file_name"])
            edge_to_attr_node = PsqlEdge(label=LABEL_MAP[attr],
                                         src_id=file_node.node_id,
                                         dst_id=attr_node.node_id)
            if not self.dryrun:
                self.pg_driver.edge_insert(edge_to_attr_node, session=session)

    def store_file_in_pg(self, filename, md5, md5_source, session):
        # not there, need to get id from signpost and store it.
        did = self.allocate_id_from_signpost()
        system_annotations = {"md5_source": md5_source,
                              "source": "tcga_dcc"}
        if not self.dryrun:
            tarinfo = self.tarball.getmember(self.full_name(filename))
            file_size = tarinfo.size
        else:
            file_size = 0
        file_node = PsqlNode(node_id=did, label="file",
                             properties={"file_name": filename,
                                         "md5sum": md5,
                                         "state": "submitted",
                                         "file_size": file_size,
                                         "state_comment": None},
                             system_annotations=system_annotations)
        edge_to_archive = PsqlEdge(label="member_of",
                                   src_id=file_node.node_id,
                                   dst_id=self.archive_node.node_id,
                                   properties={})
        # TODO tie files to the center they were submitted by.
        # skipping this for now because it's some work to find the
        # correct center for an archive
        if self.dryrun:
            self.log.info("dryrun mode, not inserting file %s as node %s", filename, file_node)
            return file_node
        self.log.info("inserting file %s as node %s", filename, file_node)
        self.pg_driver.node_insert(file_node, session=session)
        self.pg_driver.edge_insert(edge_to_archive, session=session)
        return file_node

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
        # TODO check that this matches what we know about the archive
        try:
            file_node.acl = ["phs000178"] if classification["data_access"] == "protected" else []
        except:
            self.log.error("%s has no acl but was not marked unclassified. classification: %s",
                           file_node["file_name"],
                           classification)
            raise
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

    def set_file_state(self, file_node, state):
        self.pg_driver.node_update(file_node, properties={"state": state})

    def extract_file_data(self, filename):
        return self.tarball.extractfile("/".join([self.name, filename]))

    def sync_file(self, filename, dcc_md5, session):
        """Sync this file in the database."""
        file_node = self.lookup_file_in_pg(self.archive_node,
                                           filename, session)
        if file_node:
            self.log.info("file %s in already in postgres, not inserting",
                          filename)
            return file_node
        if dcc_md5:
            md5 = dcc_md5
            md5_source = "tcga_dcc"
        else:
            md5 = md5sum(iterable_from_file(
                self.extract_file_data(filename)))
            md5_source = "gdc_import_process"
        file_node = self.store_file_in_pg(filename, md5, md5_source, session)
        self.classify(file_node, session)
        return file_node

    def get_manifest(self):
        if self.dryrun:
            resp = self.get_with_auth("/".join([self.archive["non_tar_url"],
                                                "MANIFEST.txt"]))
            manifest_data = resp.content
        else:
            manifest_tarinfo = self.tarball.getmember(
                "{}/MANIFEST.txt".format(self.name))
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
        if self.dryrun:
            NOT_PART_OF_ARCHIVE = ["Name", "Last modified",
                                   "Size", "Parent Directory"]
            resp = self.get_with_auth(self.archive["non_tar_url"])
            archives_html = html.fromstring(resp.content)
            return [elem.text for elem in archives_html.cssselect('a')
                    if elem.text not in NOT_PART_OF_ARCHIVE]
        else:
            return [name.replace(self.name + "/", "")
                    for name in self.tarball.getnames()]

    def get_with_auth(self, url, **kwargs):
        resp = requests.get(url, auth=self.dcc_auth,
                            allow_redirects=False, **kwargs)
        tries = 0
        while resp.is_redirect and tries < 5:
            # sometimes it redirects, try again. normally requests
            # does this automatically, but this doesn't work with auth
            tries += 1
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

    def download_archive(self):
        self.log.info("downloading archive %s", self.name)
        resp = self.get_with_auth(self.archive["dcc_archive_url"], stream=True)
        if int(resp.headers["content-length"]) > self.MAX_BYTES_IN_MEMORY:
            self.log.info("archive size is %s bytes, storing in "
                          "temp file on disk", resp.headers["content-length"])
            self.temp_file = tempfile.TemporaryFile(prefix=self.scratch_dir)
        else:
            self.temp_file = StringIO()
        for chunk in resp.iter_content(chunk_size=16000):
            self.temp_file.write(chunk)
        self.temp_file.seek(0)
        self.temp_file
        self.tarball = tarfile.open(fileobj=self.temp_file, mode="r:gz")

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

    def upload_data(self, fileobj, key):
        obj = self.container.upload_object_via_stream(iterable_from_file(fileobj),
                                                      key)
        return obj

    def full_name(self, filename):
        return "/".join([self.name, filename])

    def verify(self, node):
        urls = self.get_urls_from_signpost(node.node_id)
        if not urls:
            raise RuntimeError("no urls in signpost for file {}, node: {}".format(node.file_name, node))
        obj = self.obj_for(urls[0])
        expected_sum = node["md5sum"]
        actual_sum = md5sum(obj.as_stream())
        if actual_sum != expected_sum:
            self.pg_driver.node_update(node,
                                       properties={"state": "invalid",
                                                   "state_comment": "bad md5sum"})
            self.log.warning("file %s has invalid checksum", node["file_name"])
        self.set_file_state(node, "live")

    def upload(self, node):
        urls = self.get_urls_from_signpost(node.node_id)
        if urls:
            self.log.info("file %s already in signpost, skipping",
                          node["file_name"])
            return
        self.set_file_state(node, "uploading")
        name = self.full_name(node["file_name"])
        obj = self.upload_data(self.tarball.extractfile(name), name)
        new_url = url_for(obj)
        self.store_url_in_signpost(node.node_id, new_url)
        self.set_file_state(node, "validating")

    def sync(self):
        if self.archive["disease_code"] == "FPPP":
            self.log.info("%s is an FPPP archive, skipping", self.name)
            return
        self.log.info("syncing archive %s", self.name)
        self.archive["non_tar_url"] = self.archive["dcc_archive_url"].replace(".tar.gz", "")
        with self.pg_driver.session_scope() as session:
            self.archive_node = self.store_archive_in_pg(session)
            if self.get_urls_from_signpost(self.archive_node.node_id):
                self.log.info("archive %s already has urls in signpost, "
                              "assuming it's complete", self.archive)
                return
            if not self.dryrun:
                self.download_archive()
            manifest = self.get_manifest()
            filenames = self.get_files()
            if (not self.manifest_is_complete(manifest, filenames)
                and self.dryrun):
                self.log.warning("manifest for %s incomplete and "
                                 "we are in dryrun,"
                                 " skipping file sync", self.name)
                return
            file_nodes = []
            for filename in filenames:
                if filename != "MANIFEST.txt":
                    file_node = self.sync_file(filename, manifest.get(filename), session)
                    if file_node:
                        file_nodes.append(file_node)
        if not self.dryrun:
            for node in file_nodes:
                self.upload(node)
                self.verify(node)
            # finally, upload the archive itself
            self.temp_file.seek(0)
            self.upload_data(self.temp_file, "/".join(["archives", self.name]))
