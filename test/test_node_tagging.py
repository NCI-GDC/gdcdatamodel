import pytest
import yaml
from gdcdictionary import gdcdictionary
from psqlgraph.mocks import GraphFactory

from gdcdatamodel import models

factory = GraphFactory(models, gdcdictionary)

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


@pytest.fixture()
def sample_data():
    return yaml.safe_load(y)


def test_1(sample_data, g):
    nodes = factory.create_from_nodes_and_edges(
        nodes=sample_data["nodes"],
        edges=sample_data["edges"],
        unique_key="node_id",
        all_props=True,
    )

    with g.session_scope() as s:
        for n in nodes:
            s.add(n)

    with g.session_scope() as s:
        cv = g.nodes(models.Case).all()
        for c in cv:
            print(c.tag, c.version)

    with g.session_scope() as s:
        for n in nodes:
            s.delete(n)

