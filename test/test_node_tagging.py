import os
from test import helpers

import pytest
from psqlgraph import PsqlGraphDriver

from gdcdatamodel.models import basic, versioning  # noqa


@pytest.fixture(scope="module")
def bg():
    """Fixture for database driver"""

    cfg = {
        "host": os.getenv("PG_HOST", "localhost"),
        "user": os.getenv("PG_USER", "test"),
        "password": os.getenv("PG_PASS", "test"),
        "database": "dev_models",
        "package_namespace": "basic",
    }

    g = PsqlGraphDriver(**cfg)
    helpers.create_tables(g.engine, namespace="basic")
    yield g
    helpers.truncate(g.engine, namespace="basic")


@pytest.fixture(scope="module")
def create_samples(sample_data, bg):
    with bg.session_scope() as s:

        version_2s = []
        for node in sample_data:
            # delay adding version 2
            if node.node_id in [
                "a2b2d27a-6523-4ddd-8b2e-e94437a2aa23",
                "5ffb4b0e-969e-4643-8187-536ce7130e9c",
            ]:
                version_2s.append(node)
                continue
            s.add(node)
        s.commit()
        for v2 in version_2s:
            s.add(v2)
    yield

    with bg.session_scope():
        for n in sample_data:
            bg.node_delete(n.node_id)


@pytest.mark.parametrize(
    "node_id, tag, version",
    [
        (
            "be66197b-f6cc-4366-bded-365856ec4f63",
            "84044bd2-54a4-5837-b83d-f920eb97c18d",
            1,
        ),
        (
            "a2b2d27a-6523-4ddd-8b2e-e94437a2aa23",
            "84044bd2-54a4-5837-b83d-f920eb97c18d",
            2,
        ),
        (
            "813f97c4-dffc-4f94-b3f6-66a93476a233",
            "9a81bbad-b525-568c-b85d-d269a8bdc70a",
            1,
        ),
        (
            "6974c692-be47-4cb8-b8d6-9bd815983cd9",
            "55814b2f-fc23-5bed-9eab-c73c52c105df",
            1,
        ),
        (
            "5ffb4b0e-969e-4643-8187-536ce7130e9c",
            "55814b2f-fc23-5bed-9eab-c73c52c105df",
            2,
        ),
        (
            "c6a795f6-ee4a-4fcd-bfed-79348e07cd49",
            "8cc95392-5861-5524-8b98-a85e18d8294c",
            1,
        ),
        (
            "ed9aa864-1e40-4657-9378-7e3dc26551cc",
            "fddc5826-8853-5c1a-847d-5850d58ccb3e",
            1,
        ),
        (
            "fb69d25b-5c5d-4879-8955-8f2126e57524",
            "293d5dd3-117c-5a0a-8030-a428fdf2681b",
            1,
        ),
    ],
)
def test_1(create_samples, bg, node_id, tag, version):

    with bg.session_scope():
        node = bg.nodes().get(node_id)
        assert node.tag == tag
        assert node.ver == version
        assert versioning.compute_tag(node) == node.tag
