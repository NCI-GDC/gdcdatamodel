from dictionaryutils import dictionary as gdcdictionary
from jsonschema import Draft4Validator, RefResolver
import re

missing_prop_re = re.compile("\'([a-zA-Z_-]+)\' is a required property")
extra_prop_re = re.compile("Additional properties are not allowed \(u\'([a-zA-Z_-]+)\' was unexpected\)")


def get_keys(error_msg):
    missing_prop = missing_prop_re.match(error_msg)
    extra_prop = extra_prop_re.match(error_msg)
    try:
        if missing_prop:
            return [missing_prop.groups(1)[0]]
        if extra_prop:
            return [extra_prop.groups(1)[0]]
        return []
    except:
        return []


class GDCJSONValidator(object):
    def __init__(self):
        self.schemas = gdcdictionary

    def iter_errors(self, doc):
        # Note whenever gdcdictionary use a newer version of jsonschema
        # we need to update the Validator
        validator = Draft4Validator(self.schemas.schema[doc['type']])
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
                    "specified type: {} is not in the current data model"
                    .format(json_doc['type']), keys=['type'])
                break
            for error in self.iter_errors(json_doc):
                # the key will be  property.subproperty for nested properties
                keys = ['.'.join(error.path)] if error.path else []
                if not keys:
                    keys = get_keys(error.message)
                message = error.message
                if error.context:
                    message += ': {}'.format(' and '.join([c.message for c in error.context]))
                entity.record_error(message, keys=keys)
            # additional validators go here
