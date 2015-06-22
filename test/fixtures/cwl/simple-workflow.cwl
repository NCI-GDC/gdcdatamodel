class: Workflow
inputs:
- {id: '#file1', type: File}
outputs:
- connect: {source: '#step2_output'}
  id: '#count_output'
  type: int
steps:
- inputs:
  - connect: {source: '#file1'}
    param: '#wc_file1'
  outputs:
  - {id: '#step1_output', param: '#wc_output'}
  run:
    baseCommand: [wc]
    class: CommandLineTool
    inputs:
    - {id: '#wc_file1', type: File}
    outputs:
    - id: '#wc_output'
      outputBinding: {glob: output}
      type: File
    stdin: {engine: 'cwl:JsonPointer', script: job/file1/path}
    stdout: output
- inputs:
  - connect: {source: '#step1_output'}
    param: '#parseInt_file1'
  outputs:
  - {id: '#step2_output', param: '#parseInt_output'}
  run:
    class: ExpressionTool
    expression: {engine: '#nodeengine', script: '{return {''output'': parseInt($job.file1.contents)};}'}
    inputs:
    - id: '#parseInt_file1'
      inputBinding: {loadContents: true}
      type: File
    outputs:
    - {id: '#parseInt_output', type: int}
    requirements:
    - class: ExpressionEngineRequirement
      engineCommand: cwlNodeEngine.js
      id: '#nodeengine'
      requirements:
      - {class: DockerRequirement, dockerImageId: commonworkflowlanguage/nodejs-engine}
