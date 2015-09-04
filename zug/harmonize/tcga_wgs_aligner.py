from sqlalchemy import func, desc, BigInteger

from zug.binutils import NoMoreWorkException
from gdcdatamodel.models import (
    Aliquot, File, ExperimentalStrategy,
    Platform, Center,
    FileDataFromAliquot,
)

from zug.harmonize.tcga_bwa_aligner import TCGABWAAligner


class TCGAWGSAligner(TCGABWAAligner):

    @property
    def name(self):
        return "tcga_wgs_aligner"

    @property
    def source(self):
        return "tcga_wgs_alignment"

    def choose_bam_by_forced_id(self):
        input_bam = self.graph.nodes(File).ids(self.config["force_input_id"]).one()
        assert input_bam.sysan["source"] == "tcga_cghub"
        assert input_bam.file_name.endswith(".bam")
        assert input_bam.data_formats[0].name == "BAM"
        assert input_bam.experimental_strategies[0].name == "WGS"
        return input_bam

    @property
    def bam_files(self):
        '''targeted bam files query'''
        wgs = ExperimentalStrategy.name.astext == "WGS"
        hms = Center.short_name.astext == "HMS"
        illumina = Platform.name.astext.contains("Illumina")
        # NOTE you would think that file_name filter would be
        # unnecessary but we have some TCGA exomes that end with
        # .bam_HOLD_QC_PENDING. I am not sure what to do with these so
        # for now I am ignoring them
        return self.graph.nodes(File)\
                         .props(state="live")\
                         .sysan(source="tcga_cghub")\
                         .join(FileDataFromAliquot)\
                         .join(Aliquot)\
                         .distinct(Aliquot.node_id.label("aliquot_id"))\
                         .filter(File.experimental_strategies.any(wgs))\
                         .filter(File.centers.any(hms))\
                         .filter(File.platforms.any(illumina))\
                         .filter(File.file_name.astext.endswith(".bam"))\
                         .order_by(Aliquot.node_id, desc(File._sysan["cghub_upload_date"].cast(BigInteger)))

    @property
    def alignable_files(self):
        '''bam files that are not aligned'''
        currently_being_aligned = self.consul.list_locked_keys()
        return self.bam_files\
            .filter(~File.derived_files.any())\
            .filter(~File.node_id.in_(currently_being_aligned))

    def choose_bam_at_random(self):
        """This queries for a bam file that we can align at random,
        potentially filtering by size.

        """
        alignable_files = self.alignable_files
        if self.config["size_limit"]:
            alignable_files = alignable_files.filter(
                File.file_size.cast(BigInteger) < self.config["size_limit"]
            )
        input_bam = alignable_files.from_self(File).order_by(func.random()).first()
        if not input_bam:
            raise NoMoreWorkException("We appear to have aligned all bam files")
        else:
            return input_bam
