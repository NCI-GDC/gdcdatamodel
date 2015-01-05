from cStringIO import StringIO
import tempfile
import tarfile
import re
import hashlib
from contextlib import contextmanager

import requests

from libcloud.storage.drivers.s3 import S3StorageDriver
from libcloud.storage.drivers.cloudfiles import OpenStackSwiftStorageDriver

from psqlgraph import PsqlNode, PsqlEdge, session_scope


def md5sum(iterable):
    md5 = hashlib.md5()
    for chunk in iterable:
        md5.update(chunk)
    return md5.hexdigest()


class TCGADCCArchiveSyncer(object):

    MAX_BYTES_IN_MEMORY = 2 * (10**9)  # 2GB
    SIGNPOST_VERSION = "v0"

    def __init__(self, signpost_url, pg_driver, storage_client, dcc_auth, scratch_dir):
        self.signpost_url = signpost_url
        self.storage_client = storage_client
        # TODO probably make object store connection here
        self.pg_driver = pg_driver
        # container_for is a function that takes an archive and
        # returns the name of a container to store it in
        self.dcc_auth = dcc_auth
        self.scratch_dir = scratch_dir

    def put_archive_in_pg(self, archive):
        # legacy_id is just the name without the revision or series
        # this will be identical between different versions of an archive as new
        # versions are submitted
        legacy_id = re.sub("\.(\d+?)\.(\d+)$", "", archive["archive_name"])
        old_versions = self.driver.node_lookup(label="archive",
                                               property_matches={"legacy_id": legacy_id})
        if len(old_versions) > 1:
            # since we void all old versions of an archive when we add a new one,
            # there should never be more than one old version in the database
            raise ValueError("multiple old versions of an archive found")
        if old_versions:
            old_archive = old_versions[0]
            # first get all the files related to this archive and void them
            with self.driver.session_scope() as session:
                for file in self.driver.node_lookup(label="file")\
                                       .with_edge_to_node("member_of", old_archive):
                    self.driver.node_delete(file, session=session)
                self.driver.node_delete(node=file, session=session)
        new_archive_node = PsqlNode(label="archive",
                                    properties={"legacy_id": legacy_id,
                                                "revision": archive["revision"]})
        with self.driver.session_scope() as session:
            session.add(new_archive_node)
        return new_archive_node

    def get_archive_stream(self, url):
        resp = requests.get(url, stream=True, allow_redirects=False)
        if resp.is_redirect:  # redirects mean a protected archive, so use auth
            resp = requests.get(resp.headers["location"], stream=True,
                                auth=self.dcc_auth, allow_redirects=False)
        resp.raise_for_status()
        return resp

    @contextmanager
    def untar_archive(self, archive):
        resp = self.get_archive_stream(archive["dcc_archive_url"])
        if resp.headers["content-length"] > self.MAX_BYTES_IN_MEMORY:
            temp_file = tempfile.TemporaryFile(prefix=self.scratch_dir)
        else:
            temp_file = StringIO()
        for chunk in resp.iter_content():
            temp_file.write(chunk)
        with temp_file as f:
            yield tarfile.open(f, "r|gz")

    def sync_archives(self, archives):
        for archive in archives:
            self.sync_archive(archive)

    def lookup_file_in_pg(self, archive_node, filename):
        q = self.driver.node_lookup(label="file",
                                    property_matches={"file_name": filename})\
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

    def store_file_in_pg(self, archive_node, filename, md5):
        # not there, need to get id from signpost and store it.
        did = self.allocate_id_from_signpost()
        file_node = PsqlNode(node_id=did, label="file",
                             properties={"file_name": filename,
                                         "md5sum": md5,
                                         "state": "submitted"})
        edge = PsqlEdge(label="member_of",
                        src_id=file_node.node_id,
                        dst_id=archive_node.node_id)
        with session_scope(self.pg_driver.engine) as session:
            self.pg_driver.node_insert(file_node, session)
            self.pg_driver.edge_insert(edge, session)
        return file_node

    def container_for(self, archive):
        if archive["protected"]:
            return "tcga_dcc_protected"
        else:
            return "tcga_dcc_public"

    def upload_to_object_store(self, archive, tarball, tarinfo):
        # put the file in the object store
        container = self.storage_client.get_container(self.container_for(archive))
        objname = "/".join([archive["archive_name"], tarinfo.name])
        return container.upload_object_via_stream(tarball.extractfile(tarinfo),
                                                  objname)

    def url_for(self, obj):
        """Return a url for a libcloud object."""
        DRIVER_TO_SCHEME = {
            S3StorageDriver: "s3",
            OpenStackSwiftStorageDriver: "swift"
        }
        scheme = DRIVER_TO_SCHEME[obj.driver]
        host = obj.driver.connection.host
        container = obj.container.name
        name = obj.name
        url = "{scheme}://{host}/{container}/{name}".format(scheme=scheme,
                                                            host=host,
                                                            container=container,
                                                            name=name)
        # TODO I'm quite scared that this url could end up being not
        # useful, need to do some sort of validation -- maybe
        # reconstruct the object from the url when validating
        return url

    def set_file_state(self, file_node, state):
        self.driver.node_update(file_node, properties={"state": state})

    def verify_sum(self, file_node, obj, expected_sum):
        actual_sum = md5sum(obj.as_stream())
        if actual_sum != expected_sum:
            self.driver.node_update(file_node,
                                    properties={"state": "invalid",
                                                "state_comment": "bad md5sum"})
        else:
            # TODO it shouldn't actually be live yet since it hasn't
            # been classified, might need another state
            self.set_file_state(file_node, {"state": "live"})

    def sync_file(self, archive, archive_node, tarball, tarinfo, dcc_md5):
        # 1) look up file in database, if not present, insert it, getting
        # id from signpost
        # 2) put file in object store if not already there
        filename = tarinfo.name
        file_node = self.lookup_file_in_pg(archive_node, filename)
        if not file_node:
            file_node = self.store_file_in_pg(archive_node, filename, dcc_md5)
        # does signpost already know about it?
        urls = self.get_urls_from_signpost(file_node.node_id)
        if not urls:
            self.set_file_state(file_node, "uploading")
            obj = self.upload_to_object_store(archive, tarball, tarinfo)
            new_url = self.url_for(obj)
            self.store_url_in_signpost(file_node.node_id, new_url)
        self.set_file_state(file_node, "validating")
        self.verify_sum(file_node, dcc_md5)
        # TODO classify based on Junjun/Zhenyu's regexes?

    def extract_manifest(self, tarball):
        manifest_tarinfo = tarball.getmember("MANIFEST.txt")
        manifest_data = tarball.extractfile(manifest_tarinfo)
        res = {}
        for line in manifest_data.splitlines():
            md5, filename = line.split()
            res[filename] = md5
        return res

    def sync_archive(self, archive):
        archive_node = self.put_archive_in_pg(archive)
        with self.untar_archive(archive) as tarball:
            manifest = self.extract_manifest(tarball)
            for tarinfo in tarball:
                if tarinfo.name != "MANIFEST.txt":
                    self.sync_file(archive, archive_node, tarball, tarinfo, manifest[tarinfo.name])
