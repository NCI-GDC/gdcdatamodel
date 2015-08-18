from gdcdictionary import GDCDictionary
from jsonschema import Draft4Validator, RefResolver


class GDCValidator(object):
    def __init__(self):
        self.schemas = GDCDictionary()
        self.resolver = RefResolver(
            'definitions.yaml#', self.schemas.definitions)

    def record_erros(self, *args):
        pass


class GDCJSONValidator(GDCValidator):

    def iter_errors(self, doc):
        # Note whenever gdcdictionary use a newer version of schema
        # we need to update the Validator
        validator = Draft4Validator(self.schemas.schema[doc['type']],
                                    resolver=self.resolver)
        return validator.iter_errors(doc)

    def record_errrors(self, entities):
        for entity in entities:
            json_doc = entity.doc
            if 'type' not in json_doc:
                entity.record_error(
                    "'type' is a required property", key='type')
                break
            for error in self.iter_errors(json_doc):
                # the key will looks key-subkey for nested properties
                entity.record_error(error.message, key='-'.join(error.path))
            # additional validators go here


class GDCGraphValidator(GDCValidator):
    def __init__(self):
        super(GDCGraphValidator, self).__init__()
        self.required_validators = {
            'link_validator': GDCLinkValidator()
        }
        self.optional_validators = {}

    def record_errors(self, graph, entities):
        for entity in entities:
            schema = self.schemas.schema[entity.node.label]

            for validator in self.required_validators.items():
                validator.validate(schema, entity)

            for validator_name in schema.get('validators'):
                self.optional_validators[validator_name].validate()


class GDCLinkValidator(object):
    def validate(self, schema, entity):
        for link in schema['links']:
            if 'name' in link:
                self.validate_edge(link, entity)
            elif 'subgroup' in link:
                self.validate_edge_group(link, entity)

    def validate_edge_group(self, schema, entity):
        results = [self.validate_edge(group, entity)
                   for group in schema['subgroup']]
        props = [item['name'] for item in schema]

        if schema.get('required') is True:
            if all(result['length'] == 0 for result in results):
                entity.record_error(
                    "At lease one of the properties in {} should be provided"
                    .format(props), key=", ".join(props))

        if schema.get("exclusive") is True:
            targets = [result['name'] for result in results
                       if result['length'] > 0]
            if len(targets) > 1:
                entity.record_error(
                    "Can only have one of the properties in {}"
                    .format(props), key=", ".join(targets))

    def validate_edge(self, link_sub_schema, entity):
        association = link_sub_schema['name']
        node = entity.node
        targets = node[association]
        result = {'length': len(targets), 'name': association}

        if len(targets) > 0:
            multi = link_sub_schema['multiplicity']

            if multi in ['many_to_one', 'one_to_one']:
                if len(targets) > 1:
                    entity.record_error(
                        "'{}' relationship has to be {}"
                        .format(association, multi),
                        key=association)

            if multi in ['one_to_many', 'one_to_one']:
                for target in targets:
                    if len(target[link_sub_schema['backref']]) > 1:
                        entity.record_error(
                            "'{}' relationship has to be {}, target node {} already has {}"
                            .format(association, multi,
                                    target.label, link_sub_schema['backref']),
                            key=association)

            if multi == 'many_to_many':
                pass
        else:
            if link_sub_schema.get('required') is True:
                entity.record_error("'{}' is a required property".format(association),
                                    key=association)

        return result
