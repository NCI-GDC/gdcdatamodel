import json
from psqlgraph import Node, pg_property

import os
import sys
MODULE = sys.modules[__name__]

PRIMITIVE_TYPE_MAP = {
    "null": type(None),
    "boolean": bool,
    "int": int,
    "long": long,
    "double": float,
    "float": float,
    "bytes": str,
    "string": unicode,
}


def resolve_to_pg_property_args(avro_type):
    kwargs = {}
    if isinstance(avro_type, basestring):  # primitive
        args = [PRIMITIVE_TYPE_MAP[avro_type]]
    elif isinstance(avro_type, list):  # union
        args = []
        for t in avro_type:
            if not isinstance(t, basestring):
                raise TypeError("Only lists of primitive types are supported, not {}".format(avro_type))
            args.extend(resolve_to_pg_property_args(t)[0])
            if kwargs:
                raise TypeError("Do not support kwargs in lists: {}".format(avro_type))
    elif isinstance(avro_type, dict):  # complex
        if avro_type["type"] != "enum":
            raise TypeError("{} is not a supported complex type.".format(avro_type["type"]))
        else:
            args = [str]
            kwargs["enum"] = avro_type["symbols"]
    return args, kwargs


def setter_method(avro_type):
    """
    Return a pg_property setter method corresponding to the avro type
    """
    args, kwargs = resolve_to_pg_property_args(avro_type["type"])
    print avro_type, args, kwargs
    def setter_method(self, value):
        self._set_property(avro_type["name"], value)
    return pg_property(*args, **kwargs)(setter_method)


def load_class(avro_definition, module):
    """
    Load a single psqlgraph ORM class into the given module
    from an avro definition.
    """
    name = avro_definition["name"]
    class_dict = {}
    class_dict["__nonnull_properties__"] = []
    for property in avro_definition["fields"]:
        propname = property["name"]
        if propname == "id":
            continue
        if "null" not in property["type"]:
            class_dict["__nonnull_properties__"].append(propname)
        class_dict[propname] = setter_method(property)
    generated_class = type(str(name), (Node,), class_dict)
    setattr(module, name, generated_class)


def load_classes_from_file(file):
    avro_definitions = json.load(file)
    for definitinon in avro_definitions:
        load_class(definitinon, MODULE)


with open(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), "avro", "schemata", "nodes.avsc")) as f:
    load_classes_from_file(f)



from participant import Participant
# from aliquot import Aliquot
from program import Program
from project import Project
from clinical import Clinical
from center import Center
from sample import Sample
from portion import Portion
# from analyte import Analyte
from slide import Slide
from file import File
from annotation import Annotation
from archive import Archive
from tissue_source_site import TissueSourceSite
from platform import Platform
from data_type import DataType
from data_subtype import DataSubtype
from tag import Tag
from experimental_strategy import ExperimentalStrategy
from data_format import DataFormat
from publication import Publication
