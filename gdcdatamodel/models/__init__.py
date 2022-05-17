# -*- coding: utf-8 -*-

"""gdcdatamodel.models
----------------------------------

This module defines all of the ORM classes for the GDC datamodel
python layer.  Classes are produced by a factory, and required
attributes are injected into them before they are registered into the
modules globals map.

::WARNING:: This code is the heart of the GDC.  Changes here will
propogate to all code that imports this package and MAY BREAK THINGS.

- jsm

"""
import os
import sys

try:
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache

from types import ModuleType
import logging

from collections import defaultdict

from sqlalchemy.orm import configure_mappers

import hashlib
from gdcdatamodel.models import (
    versioned_nodes,
    notifications,
    submission,
    redaction,
    qcreport,
    released_data,
    studyrule,
    batch,
    versioning,
)

from sqlalchemy import (
    event,
    and_
)

from psqlgraph import (
    Node,
    Edge,
    pg_property
)

from psqlgraph import ext

from sqlalchemy.ext.hybrid import (
    Comparator,
    hybrid_property,
)

from gdcdatamodel.models.caching import (
    NOT_RELATED_CASES_CATEGORIES,
    RELATED_CASES_LINK_NAME,
    cache_related_cases_on_update,
    cache_related_cases_on_insert,
    cache_related_cases_on_delete,
    related_cases_from_cache,
    related_cases_from_parents,
)

from gdcdatamodel.models.indexes import (
    cls_add_indexes,
    get_secondary_key_indexes,
)
from gdcdatamodel.models.misc import FileReport                # noqa
from gdcdatamodel.models.versioned_nodes import VersionedNode  # noqa
from gdcdatamodel.models.utils import py3_to_bytes

logger = logging.getLogger('gdcdatamodel')

# These are properties that are defined outside of the JSONB column in
# the database, inform later code to skip these
excluded_props = ['id', 'type']


def remove_spaces(s):
    """Returns a stripped string with all of the spaces removed.

    :param str s: String to remove spaces from

    """
    return s.replace(' ', '')


def get_cls_package(package_namespace=None):
    cls_package = "gdcdatamodel.models"
    if package_namespace:
        cls_package = "{}.{}".format(cls_package, package_namespace)
    return cls_package


def register_class(cls, package_namespace=None):
    """Register a class in `globals`.  This allows us to import the ORM
    classes from :mod:`gdcdatamodel.models`

    :param cls: Any class object
    :param package_namespace: defaults to `models` but useful for custom package naming
    :returns: None

    """
    if package_namespace:
        m = get_cls_package(package_namespace)
        pkg = sys.modules.get(m)
        if not pkg:
            pkg = ModuleType(m)
            sys.modules[m] = pkg
            globals()[package_namespace] = pkg
        setattr(pkg, cls.__name__,  cls)

    else:
        globals()[cls.__name__] = cls


def get_links(schema):
    """Given a schema, pull out all of the ``links`` that this type can
    have an edge to.

    :returns: a ``dict`` of format ``{<name>: <link>}``

    """
    links = {}
    for entry in schema.get('links') or []:
        if 'subgroup' in entry:
            for link in entry['subgroup']:
                links[link['name']] = link
        else:
            links[entry['name']] = entry
    return links


def types_from_str(types):
    return [a for type_ in types for a in {
        'string': [str],
        'number': [float, int],
        'integer': [int],
        'float': [float],
        'null': [str],
        'boolean': [bool],
        'array': [list],
        None: [str],
    }[type_]]


def PropertyFactory(name, schema, key=None):
    """Returns a pg_property (psqlgraph specific type of hybrid_property)

    """
    key = name if key is None else key

    # Assert the dictionary has no references for properties
    assert '$ref' not in schema.keys(), (
        "Found a JSON reference in dictionary.  These should be resolved "
        "at gdcdictionary module load time as of 2016-02-24")

    # None is the default for a schema type.
    types = None
    if schema.get('oneOf'):
        # We will handle an empty list after the oneOf/types field checks.
        # If it really an empty list, then use None as a value.
        types = [
            oneOf['type']
            for oneOf in schema['oneOf']
            if oneOf.get('type')
        ] or None

    # If there's both overwrite the 'oneOf' field in favor of the 'type' field.
    if schema.get('type'):
        # Lookup property type and coerce to list
        types = schema.get('type')

    # If None is all we have left over, then turn it into a list of None.
    types = [types] if not isinstance(types, list) else types

    # Convert the list of string type identifiers to Python types
    python_types = types_from_str(types)

    # If there is an enum defined, grab it for pg_property validation
    enum = schema.get('enum')

    # Create pg_property setter
    @pg_property(*python_types, enum=enum)
    def setter(self, val):
        self._set_property(key, val)
    setter.__name__ = name

    return setter


def get_class_name_from_id(_id):
    return ''.join([a.capitalize() for a in _id.split('_')])


def get_class_tablename_from_id(_id):
    return 'node_{}'.format(_id.replace('_', ''))


def cls_inject_versioned_nodes_lookup(cls):
    """Injects a property and a method into the class to retrieve node
    versions.

    """

    @property
    def _versions(self):
        """Returns a query if the node is bound to a session. Raises an
          exception if the node is not bound to a session.

        :returns: A SQLAlchemy query for node versions.

        """

        session = self.get_session()
        if not session:
            raise RuntimeError(
                '{} not bound to a session. Try .get_versions(session).'
                .format(self))
        return self.get_versions(session)

    def get_versions(self, session):
        """Returns a query for node versions given a session.

        """

        return session.query(VersionedNode)\
                      .filter(VersionedNode.node_id == self.node_id)\
                      .filter(VersionedNode.label == self.label)\
                      .order_by(VersionedNode.key.desc())

    cls._versions = _versions
    cls.get_versions = get_versions


def cls_inject_created_datetime_hook(cls,
                                     updated_key="updated_datetime",
                                     created_key="created_datetime"):
    """Given a class, inject a SQLAlchemy hook that will write the
    timestamp of the last session flush to the :param:`updated_key`
    and :param:`created_key` properties.

    """

    @event.listens_for(cls, 'before_insert')
    def set_created_updated_datetimes(mapper, connection, target):
        ts = target.get_session()._flush_timestamp.isoformat('T')
        if updated_key in target.props:
            target._props[updated_key] = ts
        if created_key in target.props:
            target._props[created_key] = ts


def cls_inject_updated_datetime_hook(cls, updated_key="updated_datetime"):
    """Given a class, inject a SQLAlchemy hook that will write the
    timestamp of the last session flush to the :param:`updated_key`
    property.

    """

    @event.listens_for(cls, 'before_update')
    def set_updated_datetimes(mapper, connection, target):
        # SQLAlchemy fires this event when associations change, but we should
        # only adjust the timestamp if the object itself was modified.
        target_session = target.get_session()
        if target_session.is_modified(target, include_collections=False):
            ts = target_session._flush_timestamp.isoformat('T')
            if updated_key in target.props:
                target._props[updated_key] = ts


def cls_inject_secondary_keys(cls, schema):
    """The dictionary defines a list of ``unique`` keys.  If there are
    keys (possibly tuples of keys) in addition to the canonical `id`
    column, then we want to be able to query the database against
    these keys.

    This function injects:
        0. a list of ``str`` keys that should be unique to
           ``_pg_secondary_keys``
        0. ``_secondary_keys_dict`` which is a dictionary of str key
           to :class:`sqlalchemy.dialects.postgresql.json.JSONElement`
           values
        0. ``_secondary_keys``, a tuple of
           :class:`sqlalchemy.dialects.postgresql.json.JSONElement`
           objects

    """

    unique_keys = schema.get('uniqueKeys', [])
    cls.__pg_secondary_keys = [
        keys for keys in unique_keys if 'id' not in keys
    ]

    class SecondaryKeyComparator(Comparator):
        def __eq__(self, other):
            filters = []
            cls = self.__clause_element__()
            secondary_keys = getattr(cls, '__pg_secondary_keys', [])
            for keys, values in zip(secondary_keys, other):
                if 'id' in keys:
                    continue
                other = {
                    key: val
                    for key, val
                    in zip(keys, values)
                }
                filters.append(cls._props.contains(other))
            return and_(*filters)

    @property
    def _secondary_keys_dicts(self):
        vals = []
        secondary_keys = getattr(self, '__pg_secondary_keys', [])
        for keys in secondary_keys:
            if 'id' in keys:
                continue
            vals.append({key: getattr(self, key, None) for key in keys})
        return vals

    @hybrid_property
    def _secondary_keys(self):
        vals = []
        for keys in getattr(self, '__pg_secondary_keys', []):
            vals.append(tuple(getattr(self, key) for key in keys))
        return tuple(vals)

    @_secondary_keys.comparator
    def _secondary_keys(cls):
        return SecondaryKeyComparator(cls)

    # Set this attribute so psqlgraph doesn't treat it as a property
    _secondary_keys._is_pg_property = False
    cls._secondary_keys = _secondary_keys
    cls._secondary_keys_dicts = _secondary_keys_dicts

    cls_add_indexes(cls, get_secondary_key_indexes(cls))


def NodeFactory(_id, schema, node_cls=Node, package_namespace=None):
    """Returns a node class given a schema.

    """

    name = get_class_name_from_id(_id)
    links = get_links(schema)

    tag_props = schema.get("tagProperties")
    tag_config = schema.get("tagBuilderConfig", {})

    @property
    def node_id(self, value):
        return self.node_id

    @node_id.setter
    def node_id(self, value):
        self.node_id = value

    @property
    def tag_properties(self):
        """Keys specified in the dictionary, used for generation of tags

        Returns:
            list[str]: list of keys used to generate tags
        """
        return tag_props

    @property
    def tagging_config(self):
        """Tagging configuration instance used for checking if node instance participates in versioning

        Returns:
            versioning.TaggingConfig): config instance
        """
        return versioning.TaggingConfig(cfg=tag_config)

    def is_taggable(self):
        return self.tagging_config.is_taggable(self)

    @property
    def is_latest(self):
        """latest version of the node based on tagging

        Returns:
            bool: True if its the latest version otherwise false
        """
        return self._sysan.get(versioning.TagKeys.latest, False)

    @property
    def ver(self):
        """node version number based on tagging

        versions are computed during save or via an external script
        Returns:
            int: version number
        """
        return self._sysan.get(versioning.TagKeys.version)

    @property
    def tag(self):
        """Unique identifier for a group of nodes that are part of the same version family

        Tags are generated when a node is saved. They can be manually created using an external script
        Returns:
            str: tag value
        """
        return self._sysan.get(versioning.TagKeys.tag)

    # Pull the JSONB properties from the `properties` key
    attributes = {
        key: PropertyFactory(key, schema)
        for key, schema in schema.get('properties', {}).items()
        if key not in links
        and key not in excluded_props
    }

    skipped_dict_vals = [
        '$schema',
        'systemProperties',
        'additionalProperties',
        'links',
        'properties',
        'uniqueKeys',
        'id' ,
        'tagProperties',
        'tagBuilderConfig',
    ]
    attributes['_dictionary'] = {
        key: schema[key] for key in schema if key not in skipped_dict_vals
    }

    # _defaults: default value for specified fields in the dictionary
    attributes['_defaults'] = {
        key: values['default']
        for key, values in schema.get('properties', {}).items()
        if 'default' in values
    }

    # _pg_links are out_edges, links TO other types
    attributes['_pg_links'] = {}

    # _pg_backrefs are in_edges, links FROM other types
    attributes['_pg_backrefs'] = {}

    # _pg_edges are all edges, links to AND from other types
    attributes['_pg_edges'] = {}

    # _related_cases_from_parents: get ids of related cases from this
    # nodes's sysan
    attributes['_related_cases_from_cache'] = property(
        related_cases_from_cache
    )

    if tag_props:
        attributes[versioning.TagKeys.tag] = tag
        attributes[versioning.TagKeys.version] = ver
        attributes["tag_properties"] = tag_properties
        attributes["is_latest"] = is_latest
        attributes["tagging_config"] = tagging_config
        attributes["is_taggable"] = is_taggable

    # _related_cases_from_parents: get ids of related cases from this
    # nodes parents
    attributes['_related_cases_from_parents'] = property(
        related_cases_from_parents
    )

    # Create the Node subclass!
    cls = type(name, (node_cls,), dict(
        __module__=get_cls_package(package_namespace),
        __tablename__=get_class_tablename_from_id(_id),
        __label__=_id,
        id=node_id,
        **attributes
    ))

    cls_inject_created_datetime_hook(cls)
    cls_inject_updated_datetime_hook(cls)
    cls_inject_versioned_nodes_lookup(cls)
    cls_inject_secondary_keys(cls, schema)

    if tag_props:
        versioning.inject_set_tag_after_insert(cls)

    node_cls.add_subclass(cls)
    return cls


def generate_edge_tablename(src_label, label, dst_label):
    """Generate a name for the edge table.

    Because of the limit on table name length on PostgreSQL, we have
    to truncate some of the longer names.  To do this we concatenate
    the first 2 characters of each word in each of the input arguments
    up to 10 characters (per argument).  However, this strategy would
    very likely lead to collisions in naming.  Therefore, we take the
    first 8 characters of a hash of the full, un-truncated name
    *before* we truncate and prepend this to the truncation.  This
    gets us a name like ``edge_721d393f_LaLeSeqDaFrLaLeSeBu``.  This
    is rather an undesirable workaround. - jsm

    """

    tablename = 'edge_{}{}{}'.format(
        src_label.replace('_', ''),
        label.replace('_', ''),
        dst_label.replace('_', ''),
    )

    # If the name is too long, prepend it with the first 8 hex of it's hash
    # truncate the each part of the name
    if len(tablename) > 40:
        oldname = tablename
        logger.debug('Edge tablename {} too long, shortening'.format(oldname))
        tablename = 'edge_{}_{}'.format(
            hashlib.md5(py3_to_bytes(tablename)).hexdigest()[:8],
            "{}{}{}".format(
                ''.join([a[:2] for a in src_label.split('_')])[:10],
                ''.join([a[:2] for a in label.split('_')])[:7],
                ''.join([a[:2] for a in dst_label.split('_')])[:10],
            )
        )
        logger.debug('Shortening {} -> {}'.format(oldname, tablename))

    return tablename


def EdgeFactory(name, label, src_label, dst_label, src_dst_assoc,
                dst_src_assoc,
                node_cls=Node,
                edge_cls=Edge,
                package_namespace=None,
                _assigned_association_proxies=defaultdict(lambda: defaultdict(set))):
    """Returns an edge class.

    :param name: The name of the edge class.
    :param label: Assigned to ``edge.label``
    :param src_label: The label of the source edge
    :param dst_label: The label of the destination edge
    :param src_dst_assoc:
        The link name i.e. ``src.src_dst_assoc`` returns a list of
        destination type nodes
    :param dst_src_assoc:
        The backref name i.e. ``dst.dst_src_assoc`` returns a list of
        source type nodes
    :param _assigned_association_proxies:
        Don't pass this parameter. This will be used to store what
        links and backrefs have been assigned to the source and
        destination nodes.  This prevents clobbering a backref with a
        link or a link with a backref, as they would be from different
        nodes, should be different relationships, and would have
        different semantic meanings.

    """
    cls_package = get_cls_package(package_namespace)
    # Correctly format all of the names
    name = remove_spaces(name)
    label = remove_spaces(label)
    src_label = remove_spaces(src_label)
    dst_label = remove_spaces(dst_label)
    src_dst_assoc = remove_spaces(src_dst_assoc)
    dst_src_assoc = remove_spaces(dst_src_assoc)

    # Generate the tablename. If it is too long, it will be hashed and
    # truncated.
    tablename = generate_edge_tablename(src_label, label, dst_label)

    # Lookup the tablenames for the source and destination classes
    src_cls = node_cls.get_subclass(src_label)
    dst_cls = node_cls.get_subclass(dst_label)

    # Assert that we're not clobbering link names
    assoc_proxy_key = package_namespace
    assert dst_src_assoc not in _assigned_association_proxies[assoc_proxy_key][dst_label], (
        "Attempted to assign backref '{link}' to node '{node}' but "
        "the node already has an attribute called '{link}'"
            .format(link=dst_src_assoc, node=dst_label))
    assert src_dst_assoc not in _assigned_association_proxies[assoc_proxy_key][src_label], (
        "Attempted to assign link '{link}' to node '{node}' but "
        "the node already has an attribute called '{link}'"
            .format(link=src_dst_assoc, node=src_label))

    # Remember that we're adding this link and this backref
    _assigned_association_proxies[assoc_proxy_key][dst_label].add(dst_src_assoc)
    _assigned_association_proxies[assoc_proxy_key][src_label].add(src_dst_assoc)

    hooks_before_insert = edge_cls._session_hooks_before_insert + [
        cache_related_cases_on_insert,
    ]

    hooks_before_update = edge_cls._session_hooks_before_update + [
        cache_related_cases_on_update,
    ]

    hooks_before_delete = edge_cls._session_hooks_before_delete + [
        cache_related_cases_on_delete,
    ]

    cls = type(name, (edge_cls,), {
        '__module__': cls_package,
        '__label__': label,
        '__tablename__': tablename,
        '__src_class__': get_class_name_from_id(src_label),
        '__dst_class__': get_class_name_from_id(dst_label),
        '__src_dst_assoc__': src_dst_assoc,
        '__dst_src_assoc__': dst_src_assoc,
        '__src_table__': src_cls.__tablename__,
        '__dst_table__': dst_cls.__tablename__,
        '_session_hooks_before_insert': hooks_before_insert,
        '_session_hooks_before_update': hooks_before_update,
        '_session_hooks_before_delete': hooks_before_delete,
    })

    edge_cls.add_subclass(cls)
    register_class(cls, package_namespace)
    return cls


def load_nodes(dictionary, node_cls=None, package_namespace=None):
    """Parse all nodes from dictionary and create Node subclasses
    Args:
        dictionary: The dictionary to load
        node_cls (psqlgraph.Node): Node class definition
        package_namespace (str): package name
    """
    node_cls = node_cls or Node
    for entity, subschema in dictionary.schema.items():
        _id = subschema['id']
        name = get_class_name_from_id(_id)
        if not node_cls.is_subclass_loaded(name):
            try:
                cls = NodeFactory(_id, subschema, node_cls, package_namespace)
                register_class(cls, package_namespace)
            except Exception:
                print('Unable to load {}'.format(name))
                raise


def parse_edge(src_label,
               name,
               edge_label,
               subschema,
               link,
               dictionary,
               node_cls=Node,
               edge_cls=Edge,
               package_namespace=None):
    """Parse an edge from the dictionary and create and Edge subclass

    :returns: The outbound name of the edge

    """

    dst_label = link['target_type']
    backref = link['backref']

    src_label = subschema['id']
    if dst_label not in dictionary.schema:
        raise RuntimeError(
            "Destination '{}' for edge '{}' from '{}' not defined"
            .format(dst_label, name, src_label))

    dst_label = dictionary.schema[dst_label]['id']
    edge_name = ''.join(map(get_class_name_from_id, [
        src_label, edge_label, dst_label]))

    if edge_cls.is_subclass_loaded(name):
        return '_{}_out'.format(edge_name)

    edge = EdgeFactory(
        edge_name,
        edge_label,
        src_label,
        dst_label,
        name,
        backref,
        node_cls=node_cls,
        edge_cls=edge_cls,
        package_namespace=package_namespace
    )

    return '_{}_out'.format(edge.__name__)


def load_edges(dictionary, node_cls=Node, edge_cls=Edge, package_namespace=None):
    """Add a dictionry of links from this class

    { <link name>: {'backref': <backref name>, 'type': <source type> } }

    """

    for src_label, subschema in dictionary.schema.items():

        src_cls = node_cls.get_subclass(src_label)
        if not src_cls:
            raise RuntimeError('No source class labeled {}'.format(src_label))

        for name, link in get_links(subschema).items():
            edge_label = link['label']
            edge_name = parse_edge(
                src_label, name, edge_label, subschema, link,
                dictionary=dictionary,
                node_cls=node_cls,
                edge_cls=edge_cls,
                package_namespace=package_namespace,
            )
            src_cls._pg_links[link['name']] = {
                'edge_out': edge_name,
                'dst_type': node_cls.get_subclass(link['target_type'])
            }

    for src_cls in node_cls.get_subclasses():
        cache_case = (
            not src_cls._dictionary['category'] in NOT_RELATED_CASES_CATEGORIES
            or src_cls.get_label() in ['annotation']
        )

        if not cache_case:
            continue

        link = {
            'name': RELATED_CASES_LINK_NAME,
            'multiplicity': 'many_to_one',
            'required': False,
            'target_type': 'case',
            'label': 'relates_to',
            'backref': '_related_{}'.format(src_cls.label),
        }

        parse_edge(
            src_cls.label,
            link['name'],
            'relates_to',
            {'id': src_cls.label},
            link,
            dictionary=dictionary,
            node_cls=node_cls,
            edge_cls=edge_cls,
            package_namespace=package_namespace,
        )


def inject_pg_backrefs(dictionary, node_cls):
    """Add a dict of links to this class.  Backrefs look like:

    .. code-block::
        { <link name>: {'name': <backref name>, 'src_type': <source type> } }

    """

    for src_label, subschema in dictionary.schema.items():
        for name, link in get_links(subschema).items():
            dst_cls = node_cls.get_subclass(link['target_type'])
            dst_cls._pg_backrefs[link['backref']] = {
                'name': link['name'],
                'src_type': node_cls.get_subclass(src_label)
            }


def inject_pg_edges(node_cls):
    """Add a dict of ALL the links, to and from, each class

    .. code-block::
        { <link name>: {'backref': <backref name>, 'type': <target type> } }

    """

    def find_backref(link, src_cls):
        """Given the JSON link definition and a source class :param:`src_cls`,
        return the name of the backref

        """

        for prop, backref in link['dst_type']._pg_backrefs.items():
            if backref['src_type'] == cls:
                return prop

    def cls_inject_forward_edges(cls):
        """We should have already added the links that go OUT from this class,
        so let's add them to `_pg_edges`

        :returns: None, cls is mutated

        """

        for name, link in cls._pg_links.items():
            cls._pg_edges[name] = {
                'backref': find_backref(link, cls),
                'type': link['dst_type'],
            }

    def cls_inject_backward_edges(cls):
        """We should have already added the links that go INTO this class,
        so let's add them to `_pg_edges`

        :returns: None, cls is mutated

        """

        for name, backref in cls._pg_backrefs.items():
            cls._pg_edges[name] = {
                'backref': backref['name'],
                'type': backref['src_type'],
            }

    for cls in node_cls.get_subclasses():
        cls_inject_forward_edges(cls)
        cls_inject_backward_edges(cls)


@lru_cache(maxsize=10)
def load_dictionary(dictionary=None, package_namespace=None):
    """ Loads all classes defined in dictionary, this method is expected to be called only once
        and very early in the application lifecycle. Subsequent calls are cached
    Args:
        dictionary: gdc dictionary or an extension of it
        package_namespace (str): module namespace used to insert all class generated from the dictionary
    Raises:
        AssertionError: If method is called more than maxsize of the lru_cache, which is 10. This method should only
            be called once
    """

    if dictionary is None:
        from gdcdictionary import gdcdictionary
        dictionary = gdcdictionary

    node_cls, edge_cls = ext.register_base_class(package_namespace)

    load_nodes(dictionary, node_cls, package_namespace)
    load_edges(dictionary, node_cls, edge_cls, package_namespace)
    inject_pg_backrefs(dictionary, node_cls)
    inject_pg_edges(node_cls)
    configure_mappers()

    # register abstract node and edge in package
    if package_namespace:
        m = get_cls_package(package_namespace)
        pkg = sys.modules.get(m)
        setattr(pkg, "Node", node_cls)
        setattr(pkg, "Edge", edge_cls)


# load default dictionary
if os.environ.get("LOAD_GDC_DICTIONARY", "True") == "True":
    load_dictionary()

