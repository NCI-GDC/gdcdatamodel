import pytest
import yaml
from psqlgraph import PsqlGraphDriver

from gdcdatamodel import models
from gdcdictionary import gdcdictionary
from psqlgraph.mocks import GraphFactory

gdcdictionary.schema["program"]["tagProperties"] = ["name"]
gdcdictionary.schema["project"]["tagProperties"] = ["code"]
gdcdictionary.schema["case"]["tagProperties"] = ["submitter_id"]

models.load_dictionary(gdcdictionary, package_namespace="test")
factory = GraphFactory(models.test, gdcdictionary)

y = """
nodes:
  - label: program
    name: GDC
    node_id: pg_1
  - label: project
    name: MISC
    code: MISC
    node_id: pj_1
    disease_type: Cancer
  - label: case
    project_id: GDC-MISC
    submitter_id: SAMPLE_!
    node_id: case_1
edges:
  - src: pg_1
    dst: pj_1
  - src: pj_1
    dst: case_1
"""


@pytest.fixture(scope='module')
def tg(tables_created):
    """Fixture for database driver"""

    cfg = {
        'host': 'localhost',
        'user': 'test',
        'password': 'test',
        'database': 'automated_test',
        'package_namespace': 'test',
    }

    return PsqlGraphDriver(**cfg)


@pytest.fixture()
def sample_data():
    return yaml.safe_load(y)


def test_1(sample_data, tg):
    nodes = factory.create_from_nodes_and_edges(
        nodes=sample_data["nodes"],
        edges=sample_data["edges"],
        unique_key="node_id",
        all_props=True,
    )

    with tg.session_scope() as s:
        for n in nodes:
            s.add(n)

    with tg.session_scope() as s:
        cv = tg.nodes(models.test.Case).all()
        for c in cv:
            assert c.tag is not None
            assert c.version == 1

    with tg.session_scope() as s:
        for n in nodes:
            s.delete(n)

