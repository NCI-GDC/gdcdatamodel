import os
import abc
import time

from gdcdatamodel.models import (
    File,
    FileDataFromFile
)

from zug.harmonize.abstract_harmonizer import AbstractHarmonizer


class TCGASTARAligner(AbstractHarmonizer):
    '''
    STAR specialization of the AbstractHarmonizer for processing of TCGA
    RNA-Seq data.
    '''
    __metaclass__ = abc.ABCMeta

    def get_config(self, kwargs):
        
        if os.environ.get("ALIGNMENT_SIZE_LIMIT"):
            size_limit = int(os.environ["ALIGNMENT_SIZE_LIMIT"])
        else:
            size_limit = None
        
        genome_dir = os.environ['GENOME_DIR']
        genome_ref = os.environ['GENOME_REF']
        genome_ref_flat = os.environ['GENOME_REF_FLAT']
        genome_annotations = os.environ['GENOME_ANNOTATIONS']
        rnaseq_qc_annotations = os.environ['RNASEQ_QC_ANNOTATIONS']
        
        return {
            "output_buckets": {
                "bam": os.environ["BAM_S3_BUCKET"],
                "bai": os.environ["BAM_S3_BUCKET"],
                "log": os.environ["LOGS_S3_BUCKET"],
                "meta": os.environ["META_S3_BUCKET"],
            },
            
            'paths': {
                'genome_dir': genome_dir,
                'genome_ref': genome_ref,
                'genome_ref_flat': genome_ref_flat,
                'genome_annotations': genome_annotations,
                'rnaseq_qc_annotations': rnaseq_qc_annotations,
            },
            
            "size_limit": size_limit,
            "cores": int(os.environ.get("ALIGNMENT_CORES", "8")),
            "force_input_id": kwargs.get("force_input_id"),
        }

    @property
    def valid_extra_kwargs(self):
        return ["force_input_id"]

    @property
    def input_schema(self):
        '''
        Mapping from names to input types.
        '''
        # NOTE From what james has mentioned, this is just mappings to
        # PSQLGraph types?
        return {
            'fastq_tarball': File,
        }

    @property
    def output_schema(self):
        '''
        Mapping from names to output types.
        '''
        # NOTE The reverse of the input_schema.
        return {
            'bam': File,
            'bai': File,
            'log': File,
            # NOTE Initial pass meta will be a tarball of any excess data.
            'meta': File,
        }

    def find_inputs(self):
        '''
        Identify inputs.
        '''
        self.log.info('Choosing FASTQ file for alignment.')
        if self.config.get('force_input_id', False):
            input_fastq_tarball = self.choose_fastq_by_forced_id()
        else:
            input_fastq_tarball = self.choose_fastq_at_random()
        self.log.info('Selected %s for alignment.', input_fastq_tarball)
        
        # we expunge from this session so we can merge into another
        # session later and load up it's classifiction nodes to
        # classify the newly produced bam
        self.graph.current_session().expunge(input_fastq_tarball)
        
        return input_fastq_tarball.node_id, {
            'fastq_tarball': input_fastq_tarball,
        }

    def build_docker_cmd(self):
        '''
        Build the docker command based on configuration options.
        '''
        return ' '.join([
            '/usr/bin/python',
            '/home/ubuntu/star_docker/align_star.py',
            '--genomeDir {genome_dir}',
            '--ref_genome {genome_ref}',
            '--input {fastq_tarball}',
            '--genome_annotation {genome_annotations}',
            '--out {output_bam}',
            '--workDir {scratch_dir}',
            '--id {uuid}',
            '--ref_flat {genome_ref_flat}',
            '--runThreadN {nthreads}',
            # TODO verify same as --ref_genome in all cases
            '--genomeFastaFiles {genome_ref}',
            '--rna_seq_qc_annotation {rnaseq_qc_annotations}',
        ]).format(
            scratch_dir = self.container_abspath(self.config['scratch_dir']),
            genome_dir = self.container_abspath(self.config['genome_dir']),
            genome_ref = self.container_abspath(self.config['genome_ref']),
            genome_ref_flat = self.container_abspath(self.config['genome_ref_flat']),
            genome_annotations = self.container_abspath(self.config['genome_annotations']),
            rnaseq_qc_annotations = self.container_abspath(self.config['rnaseq_qc_annotations']),
            fastq_tarball = self.container_abspath(self.input_paths['fastq_tarball']),
            output_bam = self.container_abspath(
                self.config['scratch_dir'],
                '{uuid}.bam'.format(uuid=self.inputs['fastq_tarball'].node_id),
            ),
            uuid = self.inputs['fastq_tarball'].node_id,
            nthreads = self.config['cores'],
        )

    @property
    def output_paths(self):
        uuid = self.inputs['fastq_tarball'].node_id
        return {
            'bam': self.host_abspath(
                self.config['scratch_dir'],
                '{uuid}.bam'.format(uuid=uuid),
            ),
            'bai': self.host_abspath(
                self.config['scratch_dir'],
                '{uuid}.bam.bai'.format(uuid=uuid),
            ),
            'log': self.host_abspath(
                self.config['scratch_dir'],
                '{uuid}.log'.format(uuid=uuid),
            ),
        }

    def upload_secondary_files(self):
        """
        Upload the log file and sqlite db to the relevant bucket
        """
        path = os.path.normpath(self.host_abspath(self.output_paths['log']))
        self.upload_file(
            path,
            self.config['output_buckets']['log'],
            os.path.basename(path),
        )
        
        # TODO FIXME upload a tarball of the metadata

    def handle_output(self):
        self.upload_secondary_files()
        output_nodes = {}
        for key in ["bam", "bai"]:
            name = self.inputs[key].file_name.replace(".bam", "_gdc_realn.bam")
            output_nodes[key] = self.upload_file_and_save_to_db(
                self.host_abspath(self.output_paths[key]),
                self.config["output_buckets"][key],
                name,
                self.inputs["bam"].acl
            )
        output_nodes["bam"].related_files = [output_nodes["bai"]]
        docker_tag = (self.docker_image["RepoTags"][0]
                      if self.docker_image["RepoTags"] else None)
        edge = FileDataFromFile(
            src=self.inputs["bam"],
            dst=output_nodes["bam"],
            system_annotations={
                "alignment_started": self.start_time,
                "alignment_finished": int(time.time()),
                # raw_docker as opposed to whatever we might use in
                # the future, e.g. CWL
                "alignment_method": "raw_docker",
                "alignment_docker_image_id": self.docker_image["Id"],
                "alignment_docker_image_tag": docker_tag,
                "alignment_docker_cmd": self.docker_cmd,
                "alignment_reference_name": os.path.basename(self.config["genome_ref"]),
            }
        )
        with self.graph.session_scope() as session:
            # merge old bam file so we can get its classification
            session.add(self.inputs["bam"])
            # classify new bam file, same as the old bam file
            output_nodes["bam"].experimental_strategies = self.inputs["bam"].experimental_strategies
            output_nodes["bam"].data_formats =  self.inputs["bam"].data_formats
            output_nodes["bam"].data_subtypes = self.inputs["bam"].data_subtypes
            output_nodes["bam"].platforms = self.inputs["bam"].platforms
            # this line implicitly merges the new bam and new bai
            session.merge(edge)

    @abc.abstractmethod
    def choose_fastq_by_forced_id(self):
        '''
        Return a PSQLGraph node representing the FASTQ specified by a
        specified id.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def choose_fastq_at_random(self):
        '''
        Return a PSQLGraph node representing a 'randomly' chosen FASTQ
        '''
        raise NotImplementedError('TODO')
