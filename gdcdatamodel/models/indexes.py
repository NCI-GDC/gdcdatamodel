# -*- coding: utf-8 -*-

"""gdcdatamodel.models.indexes
----------------------------------


Specialization of PsqlGraph Node/Edge indexes specific to the GDC datamodel.

This module can be used to inject indexes into Node classes for common
query patterns (e.g. secondary_keys).

"""

from cdisutils.log import get_logger
from sqlalchemy import Index, func, text
from sqlalchemy.types import DateTime
import hashlib

logger = get_logger(__name__)


def index_name(cls, description):
    """Standardize index naming.  Because of PostgreSQL's name character
    limit, this follows a similar scheme to shortening edge names in
    `gdcdatamodel.generate_edge_tablename()`

    For long names, take the first 8 characters of a hash of the full,
    un-truncated table name *before* we truncate and prepend this to
    the truncation.  This gets us a name like
    ``index_4df72441_famihist_lower_submitte_id``.  This is yet
    another rather an undesirable workaround. - jsm

    """

    name = "index_{}_{}".format(cls.__tablename__, description)

    # If the name is too long, prepend it with the first 8 hex of it's hash
    # truncate the each part of the name
    if len(name) > 40:
        oldname = index_name
        logger.debug("Edge tablename {} too long, shortening".format(oldname))
        name = "index_{}_{}_{}".format(
            str(hashlib.md5(cls.__tablename__.encode("utf-8")).hexdigest())[:8],
            "".join([a[:4] for a in cls.label.split("_")])[:20],
            "_".join([a[:8] for a in description.split("_")])[:25],
        )

        logger.debug("Shortening {} -> {}".format(oldname, index_name))

    return name


def get_secondary_key_indexes(cls):
    """Returns tuple of indexes on the secondary keys on the class

    ..note:: THIS MUST BE CALLED AFTER `cls_inject_secondary_keys()`

    - cls._props[key].astext
    - lower(cls._props[key].astext)

    """

    #: use text_pattern_ops, allows LIKE statements not starting with %
    index_op = "text_pattern_ops"

    key_indexes = (
        Index(
            index_name(cls, key),
            cls._props[key].astext.label(key),
            postgresql_ops={key: index_op},
            unique=len(keys) == 1,
        )
        for keys in cls.__pg_secondary_keys
        for key in keys
    )

    lower_key_indexes = (
        Index(
            index_name(cls, key + "_lower"),
            func.lower(cls._props[key].astext).label(key + "_lower"),
            postgresql_ops={key + "_lower": index_op},
        )
        for keys in cls.__pg_secondary_keys
        for key in keys
    )

    # __pg_secondary_keys are "uniqueKeys" in the dictionary yaml file, they are
    # semantically supposed to be unique locally
    unique_indexes = (
        Index(
            index_name(cls, "_".join(keys) + "_uniq"),
            *(func.lower(cls._props[key].astext).label(key) for key in keys),
            postgresql_ops=dict((key, index_op) for key in keys),
            unique=True  # https://bugs.python.org/issue9232
        )
        for keys in cls.__pg_secondary_keys
        if len(keys) > 1  # skip duplicate indexes
    )

    return tuple(key_indexes) + tuple(lower_key_indexes) + tuple(unique_indexes)


def cls_add_indexes(cls, indexes):
    """Add indexes to given class"""

    list(map(cls.__table__.append_constraint, indexes))
