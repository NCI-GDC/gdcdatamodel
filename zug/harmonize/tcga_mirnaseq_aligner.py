import os
import re
import socket
import time
from datadog import statsd
from sqlalchemy import func
from queries import mirnaseq

from zug.binutils import NoMoreWorkException
from gdcdatamodel.models import (
    File, FileDataFromFile,
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
        
        reference = os.environ['REFERENCE']
        
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
            'log': File,
            'db': File,
        }

    def choose_bam_by_forced_id(self):
        input_bam = self.graph.nodes(File).ids(self.config['force_input_id']).one()
        assert input_bam.sysan['source'] == 'tcga_cghub'
        assert input_bam.data_formats[0].name == 'BAM'
        assert input_bam.experimental_strategies[0].name == 'miRNA-Seq'
        return input_bam

    @property
    def bam_files(self):
        return mirnaseq(self.graph, 'tcga_cghub')

    @property
    def alignable_files(self):
        currently_being_aligned = self.consul.list_locked_keys()
        alignable = self.bam_files\
            .props(state='live')\
            .filter(~File.derived_files.any())\
            .filter(~File.node_id.in_(currently_being_aligned))
        
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
            'log': self.host_abspath(
                self.config['scratch_dir'],
                'aln_' + self.inputs['bam'].node_id + '.log',
            ),
            'db': self.host_abspath(
                self.config['scratch_dir'],
                self.inputs['bam'].node_id + '_harmonize.db'
            ),
        }

    def submit_metrics(self):
        '''
        Submit metrics to datadog
        '''
        self.log.info('Submitting metrics')
        took = int(time.time()) - self.start_time
        input_id = self.inputs['bam'].node_id
        
        tags = [
            'alignment_type:{}'.format(self.name),
            'alignment_host:{}'.format(socket.gethostname()),
        ]
        
        statsd.event(
            '{} aligned'.format(input_id),
            'successfully aligned {} in {} minutes'.format(input_id, took / 60),
            source_type_name='harmonization',
            alert_type='success',
            tags=tags
        )
        
        with self.graph.session_scope():
            total = self.bam_files.count()
            done = self.bam_files.filter(File.derived_files.any()).count()
        
        self.log.info('%s bams aligned out of %s', done, total)
        statsd.gauge('harmonization.completed_bams',
                     done,
                     tags=tags)
        statsd.gauge('harmonization.total_bams',
                     total,
                     tags=tags)
        statsd.histogram('harmonization.seconds',
                         took,
                         tags=tags)
        statsd.histogram('harmonization.seconds_per_byte',
                         float(took) / self.inputs['bam'].file_size,
                         tags=tags)
        

    def upload_primary_files(self):
        '''
        Upload primary outputs - bams and bais.
        '''
        uuid = self.inputs['bam'].node_id
        
        docker_tag = (self.docker_image["RepoTags"][0]
                      if self.docker_image["RepoTags"] else None)
        
        output_directory = self.host_abspath(
            self.config['scratch_dir'],
            'realn/bwa_aln_s/sorted/',
        )
        
        primaries = set()
        
        # NOTE Currently the miRNA-Seq docker image produces onre or more
        # output BAM / BAI pairs, each named after their internal read group
        # name. At present, there is not a particularly easy or accurate way
        # to predict these names without inspecting the input file itself.
        # So to handle this, we simply target all BAMs and BAIs in the output
        # directory and leave the chore of handling the read group names to
        # something more suited to the task.
        for f in os.listdir(output_directory):
            fpath = os.path.join(output_directory, f)
            
            if not os.path.isfile(fpath):
                continue
            
            if not any([
                f.endswith('.bam'),
                f.endswith('.bai'),
            ]): continue
            
            primary = os.path.splitext(f)[0]
            
            primaries.add(primary)
        
        for primary in primaries:
            bam = os.path.join(output_directory, primary + '.bam')
            bai = os.path.join(output_directory, primary + '.bai')
            
            if not all([
                os.path.isfile(bam),
                os.path.isfile(bai),
            ]): raise ValueError('incomplete pairing of bams and bais')
            
            bam_node = self.upload_file_and_save_to_db(
                bam,
                self.config['output_buckets']['bam'],
                primary + '.bam',
                self.inputs['bam'].acl,
            )
            
            bai_node = self.upload_file_and_save_to_db(
                bai,
                self.config['output_buckets']['bai'],
                primary + '.bai',
                self.inputs['bam'].acl,
            )
            
            bam_node.related_files = [bai_node]
            
            edge = FileDataFromFile(
                src=self.inputs["bam"],
                dst=bam_node,
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
                session.add(bam_node)
                # classify new bam file, same as the old bam file
                bam_node.experimental_strategies = self.inputs["bam"].experimental_strategies
                bam_node.data_formats =  self.inputs["bam"].data_formats
                bam_node.data_subtypes = self.inputs["bam"].data_subtypes
                bam_node.platforms = self.inputs["bam"].platforms
                # this line implicitly merges the new bam and new bai
                session.merge(edge)

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
        tree = os.walk(self.host_abspath(
            self.config['scratch_dir'],
            'fastq',
        ))
        for root, _, files in tree:
            for f in files:
                host_f = os.path.normpath(os.path.join(root, f))
                key = os.path.join(
                    prefix,
                    os.path.relpath(
                        host_f,
                        self.host_abspath(self.config['scratch_dir']),
                    ),
                )
                
                self.upload_file(
                    host_f,
                    self.config['output_buckets']['meta'],
                    key,
                )

    def handle_output(self):
        self.upload_primary_files()
        self.upload_secondary_files(prefix=self.inputs['bam'].node_id)
        self.upload_tertiary_files(prefix=self.inputs['bam'].node_id)
