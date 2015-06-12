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
    param: wc-tool.cwl#file1
  outputs:
  - {id: '#step1_output', param: wc-tool.cwl#output}
  run: {import: wc-tool.cwl}
- inputs:
  - connect: {source: '#step1_output'}
    param: parseInt-tool.cwl#file1
  outputs:
  - {id: '#step2_output', param: parseInt-tool.cwl#output}
  run: {import: parseInt-tool.cwl}