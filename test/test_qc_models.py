import uuid

import pytest

from gdcdatamodel import models


@pytest.fixture()
def test_run(g):
    # add
    with g.session_scope() as sxn:
        tr = models.qcreport.TestRun(project_id="TEST-TEST")
        tr.entity_id = str(uuid.uuid4())
        tr.test_type = "aliquots"

        sxn.add(tr)
        yield tr

    # clean up
    with g.session_scope() as sxn:
        trx = g.nodes(models.qcreport.TestRun).get(tr.id)
        sxn.delete(trx)


def test_create_runs(g):

    tr_id = str(uuid.uuid4())

    # add
    with g.session_scope() as sxn:
        tr = models.qcreport.TestRun(project_id="TEST-TEST")
        tr.entity_id = tr_id
        tr.test_type = "aliquots"

        sxn.add(tr)

    # verify
    with g.session_scope():
        trx = g.nodes(models.qcreport.TestRun).filter(models.qcreport.TestRun.entity_id == tr_id).first()

    assert trx.id == tr.id


def test_create_validation_result(g, test_run):

    with g.session_scope():
        vr = models.qcreport.ValidationResult()
        vr.node_id = str(uuid.uuid4())
        vr.severity = "fatal"
        vr.error_type = "NO_READ_PAIR_NUMBER"
        vr.message = "The FASTQ is paired but has no read_pair_number"
        vr.node_type = "Submitted Aligned Read"
        test_run.test_results.append(vr)

    # assert
    with g.session_scope():
        vr_list = g.nodes(models.qcreport.ValidationResult)\
                    .filter(models.qcreport.ValidationResult.test_run == test_run).all()

    # single entry
    assert vr_list
    vr_1 = vr_list[0]
    assert vr.id == vr_1.id
    assert vr_1.severity == "CRITICAL"
