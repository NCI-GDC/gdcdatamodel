from sqlalchemy import BigInteger
from queries import wgs

from gdcdatamodel.models import File, Center
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
        return wgs(self.graph, 'tcga_cghub')

    @property
    def alignable_files(self):
        '''bam files that are not aligned'''
        currently_being_aligned = self.consul.list_locked_keys()
        alignable = self.bam_files\
                        .props(state="live")\
                        .not_sysan(alignment_data_problem=True)\
                        .filter(~File.derived_files.any())\
                        .filter(~File.node_id.in_(currently_being_aligned))
        if self.config["size_limit"]:
            alignable = alignable.filter(
                File.file_size.cast(BigInteger) < self.config["size_limit"]
            )
        if self.config["size_min"]:
            alignable = alignable.filter(
                File.file_size.cast(BigInteger) > self.config["size_min"]
            )
        if self.config["center_limit"]:
            # limit to just HMS for now
            hms = Center.short_name.astext == "HMS"
            alignable = alignable.filter(File.centers.any(hms))
        return alignable
