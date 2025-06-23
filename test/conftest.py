"""gdcdatamodel.test.conftest
----------------------------------

pytest setup for gdcdatamodel tests
"""

import logging
import random
import unittest
import uuid
from importlib import resources

import pytest
import sqlalchemy
import yaml
from psqlgraph import PsqlGraphDriver, mocks, psql
from sqlalchemy import create_engine, engine

from gdcdatamodel import models
from test import helpers
from test import models as test_models

models.load_dictionary(test_models.BasicDictionary, "basic")
from gdcdatamodel.models import basic  # noqa


@pytest.fixture(scope="session")
def db_config():
    return helpers.DB_CONFIG


@pytest.fixture(scope="session", autouse=True)
def setup_databases():
    """Setup the user and database"""
    print("Setting up test databases")
    root_user = helpers.DB_CONFIG_ADMIN["user"]
    host = helpers.DB_CONFIG_ADMIN["host"]
    engine = sqlalchemy.create_engine(f"postgresql://{root_user}@{host}/postgres")
    conn = engine.connect()
    _create_db_and_role(conn, "dev_models", "test", "test")
    _create_db_and_role(conn, "automated_test", "test", "test")
    conn.close()


def _create_db_and_role(conn, database: str, user: str, password: str):
    result = conn.execute(
        sqlalchemy.text("SELECT 1 FROM pg_database WHERE datname = :database"),
        database=database,
    )
    assert isinstance(result, engine.result.ResultProxy)

    row: engine.result.RowProxy = result.fetchone()
    if row is None:
        create_stmt = f'CREATE DATABASE "{database}"'
        conn.execute(create_stmt)

    try:
        user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
            user=user, password=password
        )
        conn.execute(user_stmt)

        perm_stmt = "GRANT ALL PRIVILEGES ON DATABASE {database} to {password}".format(
            database=database, password=password
        )
        conn.execute(perm_stmt)
        conn.execute("commit")
    except Exception as msg:
        logging.warning("Unable to add user:" + str(msg))


@pytest.fixture(scope="session")
def tables_created(db_config):
    """Create necessary tables"""
    engine = create_engine(
        "postgres://{user}:{pwd}@{host}/{db}".format(
            user=db_config["user"],
            host=db_config["host"],
            pwd=db_config["password"],
            db=db_config["database"],
        )
    )

    helpers.create_tables(engine)

    yield

    helpers.truncate(engine)


@pytest.fixture(scope="session")
def g(db_config, tables_created):
    """Fixture for database driver"""
    return PsqlGraphDriver(**db_config)


@pytest.fixture(scope="class")
def db_class(request, g):
    """Sets g property on a test class"""
    request.cls.g = g


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


@pytest.fixture()
def redacted_fixture(g):
    """Creates a redacted log entry"""
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
            entry = models.redaction.RedactionEntry(
                node_id=str(uuid.uuid4()), node_type="Aligned Reads"
            )
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


@pytest.mark.usefixtures("db_class")
class BaseTestCase(unittest.TestCase):
    def setUp(self):
        helpers.truncate(self.g.engine)

    def tearDown(self):
        helpers.truncate(self.g.engine)


@pytest.fixture(scope="module")
def sample_data():
    # with pkg_resources.resource_stream(__name__, "schema/data/sample.yaml") as f:
    #    graph = yaml.safe_load(f)
    with resources.files("test").joinpath("schema/data/sample.yaml").open("rb") as f:
        graph = yaml.safe_load(f)

    f = mocks.GraphFactory(basic, test_models.BasicDictionary)
    nodes = f.create_from_nodes_and_edges(
        nodes=graph["nodes"],
        edges=graph["edges"],
        unique_key="node_id",
        all_props=True,
    )

    return nodes
