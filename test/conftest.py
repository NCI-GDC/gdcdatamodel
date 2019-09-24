# -*- coding: utf-8 -*-
"""
gdcdatamodel.test.conftest
----------------------------------

pytest setup for gdcdatamodel tests
"""
import random
import unittest
import uuid

import pytest
from gdcdatamodel import models
from psqlgraph import PsqlGraphDriver, create_all, Node, Edge
from sqlalchemy import create_engine


def create_tables(engine):
    """
    create a table
    """
    create_all(engine)
    models.versioned_nodes.Base.metadata.create_all(engine)
    models.submission.Base.metadata.create_all(engine)
    models.redaction.Base.metadata.create_all(engine)
    models.qcreport.Base.metadata.create_all(engine)
    models.misc.Base.metadata.create_all(engine)


def truncate(engine):
    """
    Remove data from existing tables
    """
    conn = engine.connect()
    for table in Node.get_subclass_table_names():
        if table != Node.__tablename__:
            conn.execute('delete from {}'.format(table))
    for table in Edge.get_subclass_table_names():
        if table != Edge.__tablename__:
            conn.execute('delete from {}'.format(table))

    # Extend this list as needed
    ng_models_metadata = [
        models.versioned_nodes.Base.metadata,
        models.submission.Base.metadata,
        models.redaction.Base.metadata,
        models.qcreport.Base.metadata,
        models.misc.Base.metadata,
    ]

    for meta in ng_models_metadata:
        for table in meta.tables:
            conn.execute("DELETE FROM  {}".format(table))
    conn.close()


@pytest.fixture(scope='session')
def db_config():
    return {
        'host': 'localhost',
        'user': 'test',
        'password': 'test',
        'database': 'automated_test',
    }


@pytest.fixture(scope='session')
def tables_created(db_config):
    """
    Create necessary tables
    """
    engine = create_engine(
        "postgresql://{user}:{pwd}@{host}/{db}".format(
            user=db_config['user'], host=db_config['host'],
            pwd=db_config['password'], db=db_config['database']
        )
    )

    create_tables(engine)

    yield

    truncate(engine)


@pytest.fixture(scope='session')
def g(db_config, tables_created):
    """Fixture for database driver"""

    return PsqlGraphDriver(**db_config)


@pytest.fixture(scope='class')
def db_class(request, g):
    """
    Sets g property on a test class
    """
    request.cls.g = g


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
        log.annotation_id = str(uuid.uuid4())
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


@pytest.mark.usefixtures('db_class')
class BaseTestCase(unittest.TestCase):
    def setUp(self):
        truncate(self.g.engine)

    def tearDown(self):
        truncate(self.g.engine)
