# -*- coding: utf-8 -*-
"""
gdcdatamodel.test.conftest
----------------------------------

pytest setup for gdcdatamodel tests
"""
import random
import uuid

from gdcdatamodel import models
from psqlgraph import PsqlGraphDriver

import pytest


@pytest.fixture(scope='session')
def db_config():
    return {
        'host': 'localhost',
        'user': 'test',
        'password': 'test',
        'database': 'automated_test',
    }


@pytest.fixture(scope='session')
def g(db_config):
    """Fixture for database driver"""

    return PsqlGraphDriver(**db_config)


@pytest.fixture(scope='session')
def indexes(g):
    rows = g.engine.execute("""
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
    """).fetchall()

    return { row[0]: row[1] for row in rows }


@pytest.fixture()
def redacted_fixture(g):
    """ Creates a redacted log entry"""

    with g.session_scope() as sxn:
        log = models.redaction.RedactionLog()
        log.initiated_by = "TEST"
        log.project_id = "AB-BQ"
        log.reason = "Err"
        log.reason_category = "consent withdrawn"

        count = 0
        for i in range(random.randint(2, 5)):
            count += 1
            entry = models.redaction.RedactionEntry(node_id=str(uuid.uuid4()), node_type="Aligned Reads")
            log.entries.append(entry)

        sxn.add(log)
        sxn.commit()
    yield log.id, count

    # clean up
    with g.session_scope() as sxn:
        log = sxn.query(models.redaction.RedactionLog).get(log.id)
        # remove all entries
        for entry in log.entries:
            sxn.delete(entry)
        sxn.delete(log)
