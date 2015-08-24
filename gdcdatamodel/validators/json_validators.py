from gdcdictionary import gdcdictionary
from jsonschema import Draft4Validator, RefResolver


class GDCJSONValidator(object):
    def __init__(self):
        self.schemas = gdcdictionary
        self.resolver = RefResolver(
            'definitions.yaml#', self.schemas.definitions)

    def iter_errors(self, doc):
        # Note whenever gdcdictionary use a newer version of jsonschema
        # we need to update the Validator
        validator = Draft4Validator(self.schemas.schema[doc['type']],
                                    resolver=self.resolver)
        return validator.iter_errors(doc)

    def record_errors(self, entities):
        for entity in entities:
            json_doc = entity.doc
            if 'type' not in json_doc:
                entity.record_error(
                    "'type' is a required property", keys=['type'])
                break
            if json_doc['type'] not in self.schemas.schema:
                entity.record_error(
                    "specified type: {} is not in current data model"
                    .format(json_doc['type']), keys=['type'])
                break
            for error in self.iter_errors(json_doc):
                # the key will be  property.subproperty for nested properties
                entity.record_error(
                    error.message,
                    keys=['.'.join(error.path)] if error.path else [])
            # additional validators go here
