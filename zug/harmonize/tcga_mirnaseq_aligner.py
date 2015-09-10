import os
import re
import time
from datadog import statsd
from sqlalchemy import func, desc, BigInteger

from zug.binutils import NoMoreWorkException
from gdcdatamodel.models import (
    Aliquot, File, ExperimentalStrategy,
    Platform, Center,
    FileDataFromAliquot, FileDataFromFile
)

from zug.harmonize.abstract_harmonizer import AbstractHarmonizer


class TCGAMIRNASeqAligner(AbstractHarmonizer):

    @property
    def name(self):
        return 'tcga_mirnaseq_aligner'

    @property
    def source(self):
        return 'tcga_mirnaseq_alignment'

    def get_config(self, kwargs):
        
        if os.environ.get("ALIGNMENT_SIZE_LIMIT"):
            size_limit = int(os.environ["ALIGNMENT_SIZE_LIMIT"])
        else:
            size_limit = None
        
        if os.environ.get("ALIGNMENT_SIZE_MIN"):
            size_min = int(os.environ["ALIGNMENT_SIZE_MIN"])
        else:
            size_min = None
        
        reference = os.environ['REFERENCE']
        
        # TODO FIXME match config with salt state / needed fields
        return {
            'output_buckets': {
                'bam': os.environ['BAM_S3_BUCKET'],
                'bai': os.environ['BAM_S3_BUCKET'],
                'log': os.environ['LOGS_S3_BUCKET'],
                'db': os.environ['LOGS_S3_BUCKET'],
                'meta': os.environ['META_S3_BUCKET'],
            },
            
            'paths': {
                'reference': reference,
            },
            
            'size_limit': size_limit,
            'size_min': size_min,
            'cores': int(os.environ.get('ALIGNMENT_CORES', '8')),
            'force_input_id': kwargs.get('force_input_id'),
        }

    @property
    def valid_extra_kwargs(self):
        return ["force_input_id"]

    @property
    def input_schema(self):
        '''
        Mapping from names to input types.
        '''
        return {
            'bam': File,
        }

    @property
    def output_schema(self):
        '''
        Mapping from names to output types.
        '''
        return {
            'bam': File,
            'bai': File,
            'log': File,
            'db': File,
        }

    def choose_bam_by_forced_id(self):
        input_bam = self.graph.nodes(File).ids(self.config['force_input_id']).one()
        assert input_bam.sysan['source'] == 'tcga_cghub'
        assert input_bam.file_name.endswith('.bam')
        assert input_bam.data_formats[0].name == 'BAM'
        assert input_bam.experimental_strategies[0].name == 'miRNA-Seq'
        return input_bam

    @property
    def bam_files(self):
        mirnaseq = ExperimentalStrategy.name.astext == 'miRNA-Seq'
        illumina = Platform.name.astext.contains('Illumina')
        
        subquery = self.graph.nodes(File.node_id.label('file_id'))\
            .props(state='live')\
            .sysan(source='tcga_cghub')\
            .join(FileDataFromAliquot)\
            .join(Aliquot)\
            .distinct(Aliquot.node_id.label('aliquot_id'))\
            .filter(File.experimental_strategies.any(mirnaseq))\
            .filter(File.platforms.any(illumina))\
            .filter(File.file_name.astext.endswith('.bam'))\
            .order_by(Aliquot.node_id, desc(File._sysan['cghub_upload_date'].cast(BigInteger)))\
            .subquery()
        
        return self.graph.nodes(File).filter(File.node_id == subquery.c.file_id)

    @property
    def alignable_files(self):
        currently_being_aligned = self.consul.list_locked_keys()
        alignable = self.bam_files\
            .filter(~File.derived_files.any())\
            .filter(~File.node_id.in_(currently_being_aligned))
        
        if self.config['size_limit']:
            alignable = alignable.filter(
                File.file_size.cast(BigInteger) < self.config['size_limit']
            )
        
        if self.config['size_min']:
            alignable = alignable.filter(
                File.file_Size.cast(BigInteger) > self.config['size_min']
            )
        
        return alignable

    def choose_bam_at_random(self):
        """This queries for a bam file that we can align at random,
        potentially filtering by size.

        """
        input_bam = self.alignable_files.from_self(File).order_by(func.random()).first()
        if not input_bam:
            raise NoMoreWorkException("We appear to have aligned all bam files")
        else:
            return input_bam

    def find_inputs(self):
        '''
        Identify inputs.
        '''
        self.log.info('Choosing FASTQ file for alignment.')
        if self.config.get('force_input_id', False):
            input_bam = self.choose_bam_by_forced_id()
        else:
            input_bam = self.choose_bam_at_random()
        self.log.info('Selected %s for alignment.', input_bam)
        
        # we expunge from this session so we can merge into another
        # session later and load up it's classifiction nodes to
        # classify the newly produced bam
        self.graph.current_session().expunge(input_bam)
        
        return input_bam.node_id, {
            "bam": input_bam,
        }

    def build_docker_cmd(self):
        '''
        Build the docker command based on configuration options.
        '''
        return ' '.join([
            '/home/ubuntu/.virtualenvs/p3/bin/python',
            '/home/ubuntu/mirna-seq/alignment/realignment.py',
            '-r {reference}',
            '-b {bam}',
            '-u {uuid}',
            '-l {log_dir}',
        ]).format(
            reference = self.container_abspath(self.config['reference']),
            bam = self.container_abspath(self.input_paths['bam']),
            uuid = self.inputs['bam'].node_id,
            log_dir = self.container_abspath(self.config['scratch_dir']),
        )

    @property
    def output_paths(self):
        uuid = self.inputs['bam'].node_id
        
        return {
            'bam': self.host_abspath(
                self.config['scratch_dir'],
                'realn/bwa_aln_s/sorted/',
                '{uuid}.bam'.format(uuid=uuid),
            ),
            'bai': self.host_abspath(
                self.config['scratch_dir'],
                'realn/bwa_aln_s/sorted/',
                '{uuid}.bam.bai'.format(uuid=uuid),
            ),
            'log': self.host_abspath(
                self.config['scratch_dir'],
                'aln_' + self.inputs['bam'].node_id + '.log',
            ),
            'db': self.host_abspath(
                self.config['scratch_dir'],
                self.inputs['bam'].node_id + '_harmonize.db'
            ),
        }

    def upload_secondary_files(self, prefix=''):
        """
        Upload the log file and sqlite db to the relevant bucket
        """
        for key in ["log", "db"]:
            path = os.path.normpath(self.host_abspath(self.output_paths[key]))
            self.upload_file(
                path,
                self.config["output_buckets"][key],
                os.path.join(
                    prefix,
                    os.path.basename(path),
                ),
            )

    def upload_tertiary_files(self, prefix=''):
        '''
        Upload any remaining files.
        '''
        # TODO FIXME implement me
        pass

    def clean_input_files(self):
        '''
        Clean the scratch directory of any input files.
        '''
        # TODO FIXME implement me
        pass

    def clean_primary_files(self):
        '''
        Clean the scratch directory of any primary files.
        '''
        os.remove(self.output_paths['bam'])
        os.remove(self.output_paths['bai'])

    def clean_secondary_files(self):
        '''
        Clean the scratch directory of any secondary files.
        '''
        os.remove(self.output_paths['log'])
        os.remove(self.output_paths['db'])

    def submit_metrics(self):
        """
        Submit metrics to datadog
        """
        self.log.info("Submitting metrics")
        took = int(time.time()) - self.start_time
        input_id = self.inputs["bam"].node_id
        statsd.event(
            "{} aligned".format(input_id),
            "successfully aligned {} in {} minutes".format(input_id, took / 60),
            source_type_name="harmonization",
            alert_type="success",
        )
        with self.graph.session_scope():
            total = self.bam_files.count()
            done = self.bam_files.filter(File.derived_files.any()).count()
        self.log.info("%s bams aligned out of %s", done, total)
        frac = float(done) / float(total)
        statsd.gauge('harmonization.{}.completed_bams'.format(self.name),
                     done)
        statsd.gauge('harmonization.{}.fraction_complete'.format(self.name),
                     frac)
        statsd.histogram('harmonization.{}.seconds'.format(self.name),
                         took)
        statsd.histogram('harmonization.{}.seconds_per_byte'.format(self.name),
                         float(took) / self.inputs["bam"].file_size)

    def handle_output(self):
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
                "alignment_reference_name": os.path.basename(self.config["reference"]),
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
        
        # TODO FIXME add handles for qc upload
        self.clean_input_files()
        self.clean_primary_files()
        
        self.upload_secondary_files(prefix=output_nodes['bam'].node_id)
        self.clean_secondary_files()
        
        self.upload_tertiary_files(prefix=output_nodes['bam'].node_id)
