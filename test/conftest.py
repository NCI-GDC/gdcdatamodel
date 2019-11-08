# -*- coding: utf-8 -*-
"""
gdcdatamodel.test.conftest
----------------------------------

pytest setup for gdcdatamodel tests
"""

from psqlgraph import PsqlGraphDriver

import pytest


@pytest.fixture(scope="session")
def db_config():
    return {
        "host": "localhost",
        "user": "test",
        "password": "test",
        "database": "automated_test",
    }


@pytest.fixture(scope="session")
def g(db_config):
    """Fixture for database driver"""

    return PsqlGraphDriver(**db_config)


@pytest.fixture(scope="session")
def indexes(g):
    rows = g.engine.execute(
        """
        SELECT i.relname as indname,
               ARRAY(
               SELECT pg_get_indexdef(idx.indexrelid, k + 1, true)
               FROM generate_subscripts(idx.indkey, 1) as k
               ORDER BY k
               ) as indkey_names
        FROM   pg_index as idx
        JOIN   pg_class as i
        ON     i.oid = idx.indexrelid
        JOIN   pg_am as am
        ON     i.relam = am.oid;
    """
    ).fetchall()

    return {row[0]: row[1] for row in rows}
