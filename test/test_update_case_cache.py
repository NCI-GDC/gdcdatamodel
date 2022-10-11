"""
gdcdatamodel.test.test_update_case_cache
----------------------------------

Test functionality to update stale case caches

"""

import pytest
from gdcdatamodel import models as md

from migrations import update_case_cache


@pytest.fixture
def case_tree(g):
    """Create tree to test cache cache on"""

    case = md.Case("case")
    case.samples = [
        md.Sample("sample1"),
        md.Sample("sample2"),
    ]
    case.samples[0].portions = [
        md.Portion("portion1"),
        md.Portion("portion2"),
    ]
    case.samples[0].portions[0].analytes = [
        md.Analyte("analyte1"),
        md.Analyte("analyte2"),
    ]
    case.samples[0].portions[0].analytes[0].aliquots = [
        md.Aliquot("aliquot1"),
        md.Aliquot("alituoq2"),
    ]
    with g.session_scope() as session:
        session.merge(case)


@pytest.fixture
def case_tree_no_cache(g, case_tree):
    """Remove case cache from above tree"""

    with g.session_scope():
        kwargs = dict(synchronize_session=False)
        g.nodes(md.SampleRelatesToCase).delete(**kwargs)
        g.nodes(md.PortionRelatesToCase).delete(**kwargs)
        g.nodes(md.AnalyteRelatesToCase).delete(**kwargs)
        g.nodes(md.AliquotRelatesToCase).delete(**kwargs)


def test_update_case_cache(g, case_tree_no_cache):
    """Verify update_cache_cache_tree fixes stale case cache"""

    with g.session_scope():
        case = g.nodes(md.Case).one()
        update_case_cache.update_cache_cache_tree(g, case)

    with g.session_scope():
        nodes = g.nodes().all()
        nodes = (node for node in nodes if hasattr(node, "_related_cases"))

        for node in nodes:
            assert node._related_cases
