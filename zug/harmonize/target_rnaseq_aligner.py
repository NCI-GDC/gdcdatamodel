from sqlalchemy import func, BigInteger
from sqlalchemy.orm.query import Query

from queries import rnaseq

from zug.binutils import NoMoreWorkException
from gdcdatamodel.models import (
    File,
)

from zug.harmonize.star_aligner import STARAligner


class TARGETRNASeqAligner(STARAligner):

    @property
    def name(self):
        return "target_rnaseq_aligner"


    @property
    def source(self):
        return "target_rnaseq_alignment"

    @property
    def fastq_files(self):
        return rnaseq(self.graph, 'target_cghub')

    @property
    def alignable_files(self):
        currently_being_aligned = self.consul.list_locked_keys()
        alignable = self.fastq_files\
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
        Return a PSQLGraph node representing a 'randomly' chosen TARGET RNA-Seq
        FASTQ File node.
        '''
        fastq = self.alignable_files.from_self(File).order_by(func.random()).first()
        if not fastq:
            raise NoMoreWorkException('We appear to have aligned all fastq files')

        return fastq

    def choose_fastq_by_forced_id(self):
        '''
        Return a PSQLGraph node representing the TARGET RNA-Seq FASTQ File node
        represented by the specified id.
        '''
        forced_id = self.config.get('force_input_id')
        if forced_id is None:
            raise ValueError('forced_input_id is None')

        tar = self.graph.nodes(File).ids(forced_id).one()
        if tar is None:
            raise ValueError('could not find File node with id %s' % forced_id)

        assert tar.sysan['source'] == 'target_cghub'
        assert any(x.name in ['TAR', 'TARGZ'] for x in tar.data_formats)
        assert any(x.name == 'RNA-Seq' for x in tar.experimental_strategies)
        # TODO add additional constraint checks as necessary

        return tar
