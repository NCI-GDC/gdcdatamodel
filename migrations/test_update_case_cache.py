# -*- coding: utf-8 -*-
"""
gdcdatamodel.test.test_update_case_cache
----------------------------------

Test functionality to update stale case caches

"""

from gdcdatamodel import models as md
from psqlgraph import Node, Edge, PsqlGraphDriver

import pytest

import update_case_cache


@pytest.fixture(scope='session')
def db_config():
    return {
        'host': 'localhost',
        'user': 'test',
        'password': 'test',
        'database': 'automated_test',
    }


@pytest.fixture(scope='module')
def db_driver(db_config):
    return PsqlGraphDriver(**db_config)


@pytest.fixture(scope='module')
def case_tree(db_driver):
    """Create tree to test cache cache on"""

    case = md.Case('case')
    case.samples = [
        md.Sample('sample1'),
        md.Sample('sample2'),
    ]
    case.samples[0].portions = [
        md.Portion('portion1'),
        md.Portion('portion2'),
    ]
    case.samples[0].portions[0].analytes = [
        md.Analyte('analyte1'),
        md.Analyte('analyte2'),
    ]
    case.samples[0].portions[0].analytes[0].aliquots = [
        md.Aliquot('aliquot1'),
        md.Aliquot('alituoq2'),
    ]
    with db_driver.session_scope() as session:
        session.merge(case)


@pytest.fixture(scope='module')
def case_tree_no_cache(db_driver, case_tree):
    """Remove case cache from above tree"""

    with db_driver.session_scope():
        kwargs = dict(synchronize_session=False)
        db_driver.nodes(md.SampleRelatesToCase).delete(**kwargs)
        db_driver.nodes(md.PortionRelatesToCase).delete(**kwargs)
        db_driver.nodes(md.AnalyteRelatesToCase).delete(**kwargs)
        db_driver.nodes(md.AliquotRelatesToCase).delete(**kwargs)


def test_update_case_cache(db_driver, case_tree_no_cache):
    """Verify update_cache_cache_tree fixes stale case cache"""

    with db_driver.session_scope():
        case = db_driver.nodes(md.Case).one()
        update_case_cache.update_cache_cache_tree(db_driver, case)

    with db_driver.session_scope():
        nodes = db_driver.nodes().all()
        nodes = (node for node in nodes if hasattr(node, '_related_cases'))

        for node in nodes:
            assert node._related_cases
