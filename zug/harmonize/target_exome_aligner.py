from sqlalchemy import BigInteger

from queries import exome

from gdcdatamodel.models import File

from zug.harmonize.bwa_aligner import BWAAligner


class TARGETExomeAligner(BWAAligner):

    @property
    def name(self):
        return "target_exome_aligner"

    @property
    def source(self):
        return "target_exome_alignment"

    def choose_bam_by_forced_id(self):
        input_bam = self.graph.nodes(File).ids(self.config["force_input_id"]).one()
        assert input_bam.sysan["source"] == "target_cghub"
        assert input_bam.data_formats[0].name == "BAM"
        assert input_bam.experimental_strategies[0].name == "WXS"
        return input_bam

    @property
    def bam_files(self):
        '''targeted bam files query'''
        return exome(self.graph, "target_cghub")

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
        return alignable
