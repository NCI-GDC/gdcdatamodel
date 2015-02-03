from cdisutils.log import get_logger
from libcloud.storage.drivers.s3 import S3StorageDriver
from libcloud.storage.drivers.cloudfiles import OpenStackSwiftStorageDriver
from libcloud.storage.drivers.local import LocalStorageDriver


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


class DataLocator(object):
    def __init__(self, storage_client=None, graph=None, signpost_client=None):
        self.storage = storage_client
        self.graph = graph
        self.signpost = signpost_client
        self.log = get_logger("data locator")

    def sync(self, container):
        for obj in self.storage.list_container_objects(self.storage.get_container(container)):
            analysis_id, name = obj.name.split("/", 1)
            try:
                with self.graph.session_scope():
                    self.log.info("looking for node with submitter_id %s and name %s", analysis_id, name)
                    file_node = self.graph.nodes().props({"submitter_id": analysis_id,
                                                          "file_name": name}).one()
                doc = self.signpost.get(file_node.node_id)
                url = url_for(obj)
                doc.urls = [url]
                self.log.info("patching node %s with url %s", file_node, url)
                doc.patch()
            except:
                self.log.exception("couldn't sync %s/%s", analysis_id, name)
                continue
