from unittest import TestCase

from zug.harmonize.workflow_registry import WorkflowRegistry

from cwltool.avro_ld.validate import ValidationException


class WorkflowRegistryTest(TestCase):

    def setUp(self):
        self.registry = WorkflowRegistry()

    def test_simple_tool_regsiter(self):
        id = self.registry.register({
            'baseCommand': 'cat',
            'class': 'CommandLineTool',
            'description': u"Print the contents of a file to stdout using 'cat' running in a docker container.",
            'hints': [
                {
                    'class': 'DockerRequirement',
                    'dockerPull': 'debian:wheezy'
                }
            ],
            'inputs': [
                {
                    'id': '#file1',
                    'inputBinding': {'position': 1},
                    'type': 'File'
                },
                {
                    'id': '#numbering',
                    'inputBinding': {'position': 0, 'prefix': '-n'},
                    'type': ['null', 'boolean']}
            ],
            'outputs': []
        })
        self.assertEqual(id, 'c85ef77b-af19-5030-a3d9-ed62c6f37259')
        workflow = self.registry.get('c85ef77b-af19-5030-a3d9-ed62c6f37259')
        self.assertEqual(workflow["baseCommand"], "cat")
        self.assertEqual(workflow["outputs"], [])

    def test_simple_validation_failure(self):
        with self.assertRaises(ValidationException):
            self.registry.register({})

    def test_workflows_with_links_fail(self):
        with self.assertRaises(ValidationException):
            self.registry.register({
                'class': 'Workflow',
                'inputs': [
                    {
                        'id': '#file1',
                        'type': 'File'
                    }
                ],
                'outputs': [
                    {
                        'connect': {'source': '#step2_output'},
                        'id': '#count_output',
                        'type': 'int'
                    }
                ],
                'steps': [
                    {
                        'inputs': [
                            {
                                'connect': {'source': '#file1'},
                                'param': 'wc-tool.cwl#file1'
                            }
                        ],
                        'outputs': [
                            {
                                'id': '#step1_output',
                                'param': 'wc-tool.cwl#output'
                            }
                        ],
                        'run': {'import': 'wc-tool.cwl'}
                    },
                    {
                        'inputs': [
                            {
                                'connect': {'source': '#step1_output'},
                                'param': 'parseInt-tool.cwl#file1'}],
                        'outputs': [
                            {
                                'id': '#step2_output',
                                'param': 'parseInt-tool.cwl#output'
                            }
                        ],
                        'run': {'import': 'parseInt-tool.cwl'}
                    }
                ]
            })

    def test_handles_explicit_workflows(self):
        id = self.registry.register({
            'class': 'Workflow',
            'inputs': [
                {
                    'id': '#file1',
                    'type': 'File'
                }
            ],
            'outputs': [
                {
                    'connect': {'source': '#step2_output'},
                    'id': '#count_output',
                    'type': 'int'
                }
            ],
            'steps': [
                {
                    'inputs': [
                        {
                            'connect': {'source': '#file1'},
                            'param': '#wc_file1'
                        }
                    ],
                    'outputs': [
                        {
                            'id': '#step1_output',
                            'param': '#wc_output'
                        }
                    ],
                    'run': {'baseCommand': ['wc'],
                            'class': 'CommandLineTool',
                            'inputs': [{'id': '#wc_file1', 'type': 'File'}],
                            'outputs': [{'id': '#wc_output',
                                         'outputBinding': {'glob': 'output'},
                                         'type': 'File'}],
                            'stdin': {'engine': 'cwl:JsonPointer', 'script': 'job/file1/path'},
                            'stdout': 'output'}
                },
                {
                    'inputs': [
                        {
                            'connect': {'source': '#step1_output'},
                            'param': '#parseInt_file1'}],
                    'outputs': [
                        {
                            'id': '#step2_output',
                            'param': '#parseInt_output'
                        }
                    ],
                    'run': {'class': 'ExpressionTool',
                            'expression': {'engine': '#nodeengine',
                                           'script': "{return {'output': parseInt($job.file1.contents)};}"},
                            'inputs': [{'id': '#parseInt_file1',
                                        'inputBinding': {'loadContents': True},
                                        'type': 'File'}],
                            'outputs': [{'id': '#parseInt_output', 'type': 'int'}],
                            'requirements': [
                                {'id': '#nodeengine',
                                 'class': 'ExpressionEngineRequirement',
                                 'engineCommand': 'cwlNodeEngine.js',
                                 'requirements': [{'class': 'DockerRequirement',
                                                   'dockerImageId': 'commonworkflowlanguage/nodejs-engine'}]}

                            ]}
                }
            ]
        })
