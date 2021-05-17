import pkg_resources
import yaml


def _load(name):
    with pkg_resources.resource_stream(__name__, name) as f:
        return yaml.safe_load(f)


class Dictionary:

    def __init__(self, name):
        self.schema = _load(name)


BasicDictionary = Dictionary("schema/basic.yaml")
