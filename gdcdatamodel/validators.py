from gdcdictionary import GDCDictionary


class GDCJSONValidator(object):
    def __init__(self):
        self.schemas = GDCDictionary()

    def record_errrors(self, entities):
        for entity in entities:
            json_doc = entity.doc
            if 'type' not in json_doc:
                entity.record_error(
                    "'type' is a required property", key='type')
                break
            for error in self.schemas.iter_errors(json_doc):
                # the key will looks key-subkey for nested properties
                entity.record_error(error.message, key='-'.join(error.path))
            # additional validators go here


class GDCGraphValidator(object):
    def record_errors(self, graph, entities):
        pass
