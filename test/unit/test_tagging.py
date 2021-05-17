from gdcdatamodel.models import versioning as v
from gdcdatamodel.models import basic  # noqa


EXPECTED_TAGS = {
    "be66197b-f6cc-4366-bded-365856ec4f63": "84044bd2-54a4-5837-b83d-f920eb97c18d",
    "a2b2d27a-6523-4ddd-8b2e-e94437a2aa23": "84044bd2-54a4-5837-b83d-f920eb97c18d",
    "813f97c4-dffc-4f94-b3f6-66a93476a233": "9a81bbad-b525-568c-b85d-d269a8bdc70a",
    "6974c692-be47-4cb8-b8d6-9bd815983cd9": "55814b2f-fc23-5bed-9eab-c73c52c105df",
    "5ffb4b0e-969e-4643-8187-536ce7130e9c": "55814b2f-fc23-5bed-9eab-c73c52c105df",
    "c6a795f6-ee4a-4fcd-bfed-79348e07cd49": "8cc95392-5861-5524-8b98-a85e18d8294c",
    "ed9aa864-1e40-4657-9378-7e3dc26551cc": "fddc5826-8853-5c1a-847d-5850d58ccb3e",
    "fb69d25b-5c5d-4879-8955-8f2126e57524": "293d5dd3-117c-5a0a-8030-a428fdf2681b",
}


def test_compute_tag(sample_data):
    """Tests version tags are computed correctly per node"""

    for node in sample_data:
        print("\n..........{}...........".format(node))
        v_tag = v.compute_tag(node)
        assert v_tag == EXPECTED_TAGS[node.node_id], "invalid tag computed for {}".format(node.node_id)


def test_multi_parent(sample_data):
    """Test version tag resolves to the same value independent of how the parents were attached"""

    portion = basic.Portion(node_id="b9b6fdb3-6c31-4ed3-9f8c-67d4eae72102", submitter_id="portion_2")
    v_tag = v.compute_tag(portion)
    assert v_tag == "5776f97a-a58b-5900-83da-43cbc7105796"

    sample = center = None
    for node in sample_data:
        if node.label == "center":
            center = node
        elif node.label == "sample":
            sample = node

    if not all([center, sample]):
        assert False

    portion.samples.append(sample)
    portion.centers.append(center)
    v_tag = v.compute_tag(portion)
    assert v_tag == "a9a67fae-d916-5843-bdf3-b7db0b7a82a2"

    # unlink
    portion.samples = []
    portion.centers = []
    v_tag = v.compute_tag(portion)
    assert v_tag == "5776f97a-a58b-5900-83da-43cbc7105796"

    portion.centers.append(center)
    portion.samples.append(sample)
    v_tag = v.compute_tag(portion)
    assert v_tag == "a9a67fae-d916-5843-bdf3-b7db0b7a82a2"
