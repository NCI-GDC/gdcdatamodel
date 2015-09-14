from sqlalchemy import func, desc, BigInteger

from zug.binutils import NoMoreWorkException
from gdcdatamodel.models import (
    Aliquot, File, ExperimentalStrategy,
    Platform, Center, DataFormat, 
    FileDataFromAliquot,
)

from zug.harmonize.tcga_star_aligner import TCGASTARAligner


class TCGARNASeqAligner(TCGASTARAligner):

    @property
    def name(self):
        return "tcga_rnaseq_aligner"


    @property
    def source(self):
        return "tcga_rnaseq_alignment"

    @property
    def fastq_files(self):
        strategy = ExperimentalStrategy.name.astext == 'RNA-Seq'
        platform = Platform.name.astext.contains('Illumina')
        dataformat = DataFormat.name.astext.in_(['TAR', 'TARGZ'])
        centers = Center.short_name.astext == 'UNC'
        
        subquery = self.graph.nodes(File.node_id)\
            .sysan(source='tcga_cghub')\
            .distinct(File._sysan['cghub_legacy_sample_id'].astext)\
            .filter(File.experimental_strategies.any(strategy))\
            .filter(File.platforms.any(platform))\
            .filter(File.data_formats.any(dataformat))\
            .filter(File.centers.any(centers))\
            .order_by(
                File._sysan['cghub_legacy_sample_id'].astext,
                desc(File._sysan['cghub_upload_date'].cast(BigInteger)),
            )\
            .subquery()
        
        return self.graph.nodes(File).filter(File.node_id == subquery.c.node_id)

    @property
    def alignable_files(self):
        currently_being_aligned = self.consul.list_locked_keys()
        alignable = self.bam_files\
            .props(state='live')\
            .filter(~File.derived_files.any())\
            .filter(~File.node_id.in_(currently_being_aligned))
        
        size_limit = self.config.get('size_limit', False)
        if size_limit:
            alignable = alignable.filter(
                File.file_size.cast(BigInteger) < size_limit
            )
        
        size_min = self.config.get('size_min', False)
        if size_min:
            alignable = alignable.filter(
                File.file_size.cast(BigInteger) > size_min
            )
        
        return alignable

    def choose_fastq_at_random(self):
        '''
        Return a PSQLGraph node representing a 'randomly' chosen TCGA RNA-Seq
        FASTQ File node.
        '''
        fastq = self.alignable_files.from_self(File).order_by(func.random()).first()
        if not fastq:
            raise NoMoreWorkException('We appear to have aligned all fastq files')
        
        return fastq

    def choose_fastq_by_forced_id(self):
        '''
        Return a PSQLGraph node representing the TCGA RNA-Seq FASTQ File node
        represented by the specified id.
        '''
        forced_id = self.config.get('force_input_id')
        if forced_id is None:
            raise ValueError('forced_input_id is None')
        
        tar = self.graph.nodes(File).ids(forced_id).one()
        if tar is None:
            raise ValueError('could not find File node with id %s' % forced_id)
        
        assert tar.sysan['source'] == 'tcga_cghub'
        assert any(x.name in ['TAR', 'TARGZ'] for x in tar.data_formats)
        assert any(x.name == 'RNA-Seq' for x in tar.experimental_strategies)
        # TODO add additional constraint checks as necessary
        
        return tar
