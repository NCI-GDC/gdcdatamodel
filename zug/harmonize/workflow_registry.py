from cwltool.avro_ld.validate import validate_ex as cwl_validate_ex
from cwltool.avro_ld.ref_resolver import Loader
from cwltool.avro_ld.validate import ValidationException
from cwltool.process import get_schema
from cwltool.avro_ld.jsonld_context import avrold_to_jsonld_context
import json
import copy
from uuid import uuid5, UUID


from cdisutils.log import get_logger


WORKFLOW_NAMESPACE = UUID('8d5abff8-87c7-4499-8b62-9610f1320d09')


CWL_SCHEMA, CWL_SCHEMA_NAMES = get_schema()
CWL_JSONLD_CONTEXT, CWL_RDF_GRAPH = avrold_to_jsonld_context(CWL_SCHEMA)


WORKFLOW_CLASSES = [
    "CommandLineTool",
    "ExpressionTool",
    "Workflow",
]


class WorkflowRegistry(object):
    """The way I am envisioning this, it will eventually be a network
    service that lives behind an HTTP interface. For now just to get
    started, we have an in-memory, content addressed store of CWL
    workflows. The idea is that you register a workflow and it's
    assigned a content addressed id (uuid5 hash of it's stringified
    representation). We can then store {"aligned_with": $WORKFLOW_ID}
    on the bam files produced. Content addressing is nice because
    anyone in the future can know for sure if a given workflow was
    used to produce it, even if we migrate to a different system for
    storing workflows, disappear entirely, etc.
    """

    def __init__(self):
        self.store = {}
        self.log = get_logger("workflow_registry")
        self.cwl_loader = Loader()
        # the rest of this is a copy/paste from cwltool/main.py this
        # information could be known statically, this should be
        # refactored into the library
        url_fields = []
        for c in CWL_JSONLD_CONTEXT:
            if (c != "id" and (CWL_JSONLD_CONTEXT[c] == "@id")
                or (isinstance(CWL_JSONLD_CONTEXT[c], dict)
                    and CWL_JSONLD_CONTEXT[c].get("@type") == "@id")):
                url_fields.append(c)
        self.cwl_loader.url_fields = url_fields
        self.cwl_loader.idx["cwl:JsonPointer"] = {}

    def validate(self, workflow):
        """
        Validate a workflow per CWL.
        """
        self.log.info("Validating new workflow")
        # Since we haven't resolved links, this basically verifies
        # that there are no links, which is what we want. We disallow
        # links because the whole point here is reproducibility and
        # links can change, 404, etc. No links allowed, the entire
        # workflow has to be specified in the json doc passed in.

        to_validate_links = copy.deepcopy(workflow)
        try:
            self.cwl_loader.resolve_all(to_validate_links, "workflow")
            self.cwl_loader.validate_links(to_validate_links)
        except Exception as e:
            raise ValidationException(e.message)
        # Next, we need to validate the workflow according to the relevant avro schema
        klass = workflow.get("class")
        if not klass:
            # TODO kind of sketchy to reuse this exception class but w/e
            raise ValidationException("Workflow must have a 'class' key")
        if klass not in WORKFLOW_CLASSES:
            raise ValidationException("Workflow class must be "
                                      "one of {}, not {}.".format(WORKFLOW_CLASSES, klass))
        # This is sadly a copy-paste from cwltool. The namespace
        # argument to get_name being an empty string is somewhat
        # concerning in case they add a namespace later
        klass_schema = CWL_SCHEMA_NAMES.get_name(klass, "")
        cwl_validate_ex(klass_schema, workflow)

    def get(self, id):
        return self.store.get(id)

    def register(self, workflow):
        """Register a workflow (python dict). It must be json-serializble as
        ascii and be valid according to the CWL avro schema.
        """
        # This raises an exception if validation fails
        self.validate(workflow)
        # Generate a consistent string to hash. This is the sketchiest
        # part of the whole operation and I am somewhat concerned that
        # it won't work as well as I hope it will.  To guard against
        # changing behavior, we explicitly set all kwargs even when
        # they have the right default.
        # TODO try/catch here?
        self.log.info("Encoding new workflow as JSON")
        workflow_str = json.dumps(
            workflow,
            skipkeys=False,
            ensure_ascii=True,
            check_circular=True,
            # Let's not allow nans and Infinity in workflows
            allow_nan=False,
            # the most important one, this ensures that we get a consistent hash
            sort_keys=True,
            # we don't care about pretty-printing
            indent=None,
            # this doesn't really matter but it protects us against
            # the library changing it's defaults and makes the string shorted
            separators=(',', ':'),
            # We'll only accept ascii to preserve sanity
            encoding='ascii',
        )
        uuid = str(uuid5(WORKFLOW_NAMESPACE, workflow_str))
        self.log.info("Workflow id is %s, storing", uuid)
        self.store[uuid] = workflow
        return uuid

# singleton registry
REGISTRY = WorkflowRegistry()


class WorkflowRegistryClient(object):
    def __init__(self):
        self.registry = REGISTRY

    def register(self, workflow):
        return self.registry.register(workflow)

    def get(self, id):
        return self.registry.get(id)
