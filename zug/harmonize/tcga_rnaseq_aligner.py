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


    def choose_tar_at_random(self):
        '''
        Return a PSQLGraph node representing a 'randomly' chosen TCGA RNA-Seq
        FASTQ File node.
        '''
        # TODO verify that this filters correctly for TCGA RNA-Seq FASTQ files
        rnaseq = ExperimentalStrategy.name.astext == "RNA-Seq"
        illumina = Platform.name.astext.contains("Illumina")
        tars = DataFormat.name.astext.in_(["TAR", "TARGZ"])
        unc = Center.short_name.astext == "UNC"
        currently_being_aligned = self.consul.list_locked_keys()
        order = desc(File._sysan["cghub_upload_date"].cast(BigInteger)) 
        
        files = self.graph.nodes(File)\
            .props(state="live")\
            .sysan(source="tcga_cghub")\
            .join(FileDataFromAliquot)\
            .join(Aliquot)\
            .distinct(Aliquot.node_id.label("aliquot_id"))\
            .filter(File.experimental_strategies.any(rnaseq))\
            .filter(File.platforms.any(illumina))\
            .filter(File.data_formats.any(tars))\
            .filter(File.centers.any(unc))\
            .filter(~File.derived_files.any())\
            .filter(~File.node_id.in_(currently_being_aligned))\
            .order_by(Aliquot.node_id, order)
        
        size_limit = self.config.get('size_limit', False)
        if size_limit:
            files = files.filter(
                File.file_size.cast(BigInteger) < size_limit
            )
        
        tar = files.from_self(File).order_by(func.random()).first()
        
        if tar is None:
            raise NoMoreWorkException('Could not find any unprocessed FASTQs.')
        
        return tar
