from dictionaryutils import dictionary as gdcdictionary
import psqlgraph
import sqlalchemy


class GDCGraphValidator(object):
    '''
    Validator that validates entities' relationship with existing nodes in
    database.

    '''
    def __init__(self):
        self.schemas = gdcdictionary
        self.required_validators = {
            'links_validator': GDCLinksValidator(),
            'uniqueKeys_validator': GDCUniqueKeysValidator(),
        }
        self.optional_validators = {}

    def record_errors(self, graph, entities):
        for validator in self.required_validators.values():
            validator.validate(entities, graph)

        for entity in entities:
            schema = self.schemas.schema[entity.node.label]
            validators = schema.get('validators')
            if validators:
                for validator_name in validators:
                    self.optional_validators[validator_name].validate()


class GDCLinksValidator(object):

    def validate(self, entities, graph=None):
        for entity in entities:
            for link in gdcdictionary.schema[entity.node.label]['links']:
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

        if schema.get('required') is True and len(submitted_links) == 0:
            names = ", ".join(
                schema_links[:-2] + [" or ".join(schema_links[-2:])])
            entity.record_error(
                "Entity is missing a required link to {}"
                .format(names), keys=schema_links)

        if schema.get("exclusive") is True and len(submitted_links) > 1:
            names = ", ".join(
                schema_links[:-2] + [" and ".join(schema_links[-2:])])
            entity.record_error(
                "Links to {} are exclusive.  More than one was provided."
                .format(schema_links), keys=schema_links)

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
                        keys=[association])

            if multi in ['one_to_many', 'one_to_one']:
                for target in targets:
                    if len(target[link_sub_schema['backref']]) > 1:
                        entity.record_error(
                            "'{}' link has to be {}, target node {} already has {}"
                            .format(association, multi,
                                    target.label, link_sub_schema['backref']),
                            keys=[association])

            if multi == 'many_to_many':
                pass
        else:
            if link_sub_schema.get('required') is True:
                entity.record_error(
                    "Entity is missing required link to {}"
                    .format(association),
                    keys=[association])
        return result


class GDCUniqueKeysValidator(object):

    def validate(self, entities, graph=None):
        for entity in entities:
            schema = gdcdictionary.schema[entity.node.label]
            node = entity.node
            for keys in schema['uniqueKeys']:
                props = {}
                if keys == ['id']:
                    continue
                for key in keys:
                    prop = schema['properties'][key].get('systemAlias')
                    if prop:
                        props[prop] = node[prop]
                    else:
                        props[key] = node[key]
                if graph.nodes(type(node)).props(props).count() > 1:
                        entity.record_error(
                            '{} with {} already exists in the GDC'
                            .format(node.label, props), keys=props.keys()
                        )
