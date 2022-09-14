import pytest
from gdcdatamodel import models


@pytest.mark.parametrize("namespace, expectation", [
    (None, "gdcdatamodel.models"),
    ("d1", "gdcdatamodel.models.d1"),
    ("d2", "gdcdatamodel.models.d2"),
], ids=["default module", "custom 1", "custom 2"])
def test_get_package_for_class(namespace, expectation):
    """ Tests retrieving the proper module to insert generated classes for a given dictionary
        Args:
            namespace (str): package namespace used to logically divided classes
            expectation (str): final module name
    """

    module_name = models.get_cls_package(package_namespace=namespace)
    assert expectation == module_name


def test_loading_same_dictionary():
    """ Tests loading gdcdictionary into a different namespace,
        even though it might already be loaded into the default
    """
    # assert the custom module does not currently exist
    with pytest.raises(ImportError):
        from gdcdatamodel.models import gdcx  # noqa

    models.load_dictionary(dictionary=None, package_namespace="gdcx")

    # assert module & models now exist
    from gdcdatamodel.models import gdcx  # noqa
    assert gdcx.Project and gdcx.Program and gdcx.Case


def test_case_cache_related_edge_resolution():

    def_ns = models.caching.get_related_case_edge_cls(models.AlignedReads())
    def_class_name = "{}.{}".format(def_ns.__module__, def_ns.__name__)

    assert "gdcdatamodel.models.AlignedReadsRelatesToCase" == def_class_name

    models.load_dictionary(dictionary=None, package_namespace="gdc")
    from gdcdatamodel.models import gdc  # noqa
    ns = models.caching.get_related_case_edge_cls(gdc.AlignedReads())
    class_name = "{}.{}".format(ns.__module__, ns.__name__)
    assert "gdcdatamodel.models.gdc.AlignedReadsRelatesToCase" == class_name
