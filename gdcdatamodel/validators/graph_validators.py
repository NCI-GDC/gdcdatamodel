from gdcdictionary import GDCDictionary


class GDCGraphValidator(object):

    '''
    

    '''
    def __init__(self):
        self.schemas = GDCDictionary()
        self.required_validators = {
            'link_validator': GDCLinkValidator()
        }
        self.optional_validators = {}

    def record_errors(self, graph, entities):
        for entity in entities:
            schema = self.schemas.schema[entity.node.label]

            for validator in self.required_validators.values():
                validator.validate(schema, entity)

            validators = schema.get('validators')
            if validators:
                for validator_name in validators:
                    self.optional_validators[validator_name].validate()


class GDCLinkValidator(object):

    def validate(self, schema, entity):
        for link in schema['links']:
            if 'name' in link:
                self.validate_edge(link, entity)
            elif 'subgroup' in link:
                self.validate_edge_group(link, entity)

    def validate_edge_group(self, schema, entity):
        submitted_links = []
        schema_links = []
        num_of_edges = 0
        for group in schema['subgroup']:
            if 'subgroup' in schema['subgroup']:
                # nested subgroup
                result = self.validate_edge_group(group, entity)
            if 'name' in group:
                result = self.validate_edge(group, entity)

            if result['length'] > 0:
                submitted_links.append(result)
                num_of_edges += result['length']
            schema_links.append(result['name'])

        
        if schema.get('required') is True:
            if len(submitted_links) == 0:
                entity.record_error(
                    "At lease one of the properties in {} should be provided"
                    .format(schema_links), key=", ".join(schema_links))

        if schema.get("exclusive") is True:
            if len(submitted_links) > 1:
                entity.record_error(
                    "Can only have one of the properties in {}"
                    .format(schema_links), key=", ".join(schema_links))

        result = {'length': num_of_edges, 'name': ", ".join(schema_links)}

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
                        "'{}' link has to be {}"
                        .format(association, multi),
                        key=association)

            if multi in ['one_to_many', 'one_to_one']:
                for target in targets:
                    if len(target[link_sub_schema['backref']]) > 1:
                        entity.record_error(
                            "'{}' link has to be {}, target node {} already has {}"
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
