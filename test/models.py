from importlib import resources

import yaml


def _load(name):
    with resources.files("test").joinpath(name).open("rb") as f:
        return yaml.safe_load(f)


class Dictionary:
    def __init__(self, name):
        self.schema = _load(name)


BasicDictionary = Dictionary("schema/basic.yaml")
