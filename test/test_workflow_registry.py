from unittest import TestCase
from base import TEST_DIR

import os
import yaml

from zug.harmonize.workflow_registry import WorkflowRegistry

from cwltool.avro_ld.validate import ValidationException

FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "cwl")


class WorkflowRegistryTest(TestCase):

    def setUp(self):
        self.registry = WorkflowRegistry()

    def get_cwl_fixture(self, name):
        with open(os.path.join(FIXTURES_DIR, name)) as f:
            return yaml.load(f)

    def test_simple_tool_regsiter(self):
        id = self.registry.register(
            self.get_cwl_fixture("simple-tool.cwl")
        )
        self.assertEqual(id, 'c85ef77b-af19-5030-a3d9-ed62c6f37259')
        workflow = self.registry.get('c85ef77b-af19-5030-a3d9-ed62c6f37259')
        self.assertEqual(workflow["baseCommand"], "cat")
        self.assertEqual(workflow["outputs"], [])

    def test_get_input_schema(self):
        id = self.registry.register(
            self.get_cwl_fixture("simple-tool.cwl")
        )
        schema = self.registry.get_input_schema(id)
        # TODO better test
        self.assertEqual(len(schema["fields"]), 2)


    def test_simple_validation_failure(self):
        with self.assertRaises(ValidationException):
            self.registry.register({})

    def test_workflow_with_links_fails(self):
        with self.assertRaises(ValidationException):
            self.registry.register(
                self.get_cwl_fixture("with-links.cwl")
            )

    def test_explicit_workflows_succeed(self):
        id = self.registry.register(
            self.get_cwl_fixture("simple-workflow.cwl")
        )
        self.assertEqual(id, "a7af6799-d7ba-5c7e-b46b-6eb0f58fc536")

    def test_workflow_with_broken_link_fails(self):
        with self.assertRaises(ValidationException):
            self.registry.register(
                self.get_cwl_fixture("broken-link-workflow.cwl")
            )

    def test_flagant_schema_violation(self):
        with self.assertRaises(ValidationException):
            self.registry.register(
                self.get_cwl_fixture("violates-schema.cwl")
            )
