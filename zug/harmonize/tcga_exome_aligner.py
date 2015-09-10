from sqlalchemy import desc, BigInteger

from gdcdatamodel.models import (
    File, ExperimentalStrategy,
    Platform, DataFormat
)

from zug.harmonize.tcga_bwa_aligner import TCGABWAAligner


class TCGAExomeAligner(TCGABWAAligner):

    @property
    def name(self):
        return "tcga_exome_aligner"

    @property
    def source(self):
        return "tcga_exome_alignment"

    def choose_bam_by_forced_id(self):
        input_bam = self.graph.nodes(File).ids(self.config["force_input_id"]).one()
        assert input_bam.sysan["source"] == "tcga_cghub"
        assert input_bam.data_formats[0].name == "BAM"
        assert input_bam.experimental_strategies[0].name == "WXS"
        return input_bam

    @property
    def bam_files(self):
        '''targeted bam files query'''
        wxs = ExperimentalStrategy.name.astext == "WXS"
        illumina = Platform.name.astext.contains("Illumina")
        bam = DataFormat.name.astext == "BAM"
        all_file_ids_sq = self.graph.nodes(File.node_id)\
                                    .sysan(source="tcga_cghub")\
                                    .distinct(File._sysan["cghub_legacy_sample_id"].astext)\
                                    .filter(File.experimental_strategies.any(wxs))\
                                    .filter(File.platforms.any(illumina))\
                                    .filter(File.data_formats.any(bam))\
                                    .order_by(File._sysan["cghub_legacy_sample_id"].astext,
                                              desc(File._sysan["cghub_upload_date"].cast(BigInteger)))\
                                    .subquery()
        return self.graph.nodes(File).filter(File.node_id == all_file_ids_sq.c.node_id)

    @property
    def alignable_files(self):
        '''bam files that are not aligned'''
        currently_being_aligned = self.consul.list_locked_keys()
        alignable = self.bam_files\
                        .props(state="live")\
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
