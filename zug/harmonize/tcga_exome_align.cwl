- id: "#main"
  class: CommandLineTool
  requirements:
    - class: DockerRequirement
      dockerImageId: 722e467ddaf45d41e02b35af0115f74ea2c1394435b2e6a85edc4faf281ab31b
  baseCommand: ["/home/ubuntu/.virtualenvs/p3/bin/python", "/home/ubuntu/apipe/aln.py", "-d"]
  inputs:
    - id: "#reference"
      type: File
      description: "a reference genome, e.g. GRCh38.d1.vd1.fa, and it's associated indicies, e.g. GRCh38.d1.vd1.fa.amb"
      inputBinding:
        position: 0
        prefix: '-r'
        secondaryFiles:
          - "^.dict"
          - ".amb"
          - ".ann"
          - ".bwt"
          - ".fai"
          - ".pac"
          - ".sa"
    - id: "#input_bam"
      type: File
      description: "a bam file, e.g. cool_data.bam and it's associated index, cool_data.bam.bai"
      inputBinding:
        position: 1
        prefix: '-b'
        secondaryFiles:
          - ".bai"
    - id: "#file_id"
      type: string
      description: "unique id for this file (CGHub analysis id or GDC uuid)"
      inputBinding:
        position: 2
        prefix: '-u'
    - id: "#num_threads"
      type: int
      description: "number of threads to use for bwa-mem"
      inputBinding:
        position: 3
        prefix: '-t'
  outputs:
    - id: "#aligned_bam"
      type: File
      description: "the final aligned bam file"
      secondaryFiles:
        "^.bai"
      outputBinding:
        glob: "realn/bwa_mem_pe/md/*.bam"  # TODO *.bam is a bit too forgiving, should probably put the filename in here somehow
    - id: "#aligned_bam_md5"
      type: File
      description: "file containing the md5sum of the final aligned bam file"
      loadContents: true
      outputBinding:
        glob: "realn/bwa_mem_pe/md/*.bam.md5"
    - id: "#aligned_bai_md5"
      type: File
      description: "file containing the md5sum of the final bai index file"
      loadContents: true
      outputBinding:
        glob: "realn/bwa_mem_pe/md/*.bam.bai.md5"
