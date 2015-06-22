class: CommandLineTool
baseCommand: 3
description: "Print the contents of a file to stdout using 'cat' running in a docker container."
hints:
- {class: DockerRequirement, dockerPull: 'debian:wheezy'}
inputs:
- id: '#file1'
  inputBinding: {position: 1}
  type: File
- id: '#numbering'
  inputBinding: {position: 0, prefix: -n}
  type: ['null', boolean]
outputs: []