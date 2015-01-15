import csv
import uuid
import os

from psqlgraph import PsqlNode, PsqlEdge

from zug.datamodel import PKG_DIR


def import_code_table(graph, path, label, **kwargs):
    with open(path, 'r') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            graph.node_merge(
                node_id=row[0],
                label=label,
                properties={
                    key: row[index]
                    for key, index in kwargs.items()
                }
            )


def import_center_codes(graph, path):
    import_code_table(
        graph, path, 'center',
        legacy_id=1,
        namespace=2,
        center_type=3,
        name=4,
        short_name=4,
    )


def import_tissue_source_site_codes(graph, path):
    import_code_table(
        graph, path, 'tissue_source_site',
        legacy_id=1,
        name=2,
        project=3,
        bcr_id=4,
    )

DATA_TYPES = {
    "Copy number variation": [
        "Copy number segmentation", "Copy number estimate",
        "Normalized intensities", "Copy number germline variation",
        "LOH", "Copy number QC metrics", "Copy number variation"
    ],
    "Simple nucleotide variation": [
        "Simple germline variation", "Genotypes", "Simple somatic mutation",
        "Simple nucleotide variation"
    ],
    "Gene expression": [
        "Gene expression quantification", "miRNA quantification",
        "Isoform expression quantification", "Exon junction quantification",
        "Exon quantification"
    ],
    "Protein expression": [
        "Protein expression quantification", "Expression control"
    ],
    "Raw microarray data": [
        "Raw intensities", "Normalized intensities", "CGH Array QC",
        "Intensities Log2Ratio", "Expression control", "Intensities"
    ],
    "DNA methylation": [
        "Methylation beta value", "Bisulfite sequence alignment",
        "Methylation percentage"
    ],
    "Clinical": [
        "Tissue slide image", "Clinical data", "Biospecimen data",
        "Diagnostic image", "Pathology report"
    ],
    "Structural rearrangement": [
        "Structural germline variation", "Structural variation"
    ],
    "Raw sequencing data": [
        "Coverage WIG", "Sequencing tag", "Sequencing tag counts"
    ],
    "Other": [
        "Microsattelite instability", "ABI sequence trace", "HPV Test",
        "Test description", "Auxiliary test"
    ]
}

TAGS = [
    "meth", "hg18", "hg19", "germline", "snv", "exon", "image",
    "miRNA", "ismpolish", "byallele", "raw", "tangent", "isoform",
    "coverage", "junction", "cnv", "msi", "cgh", "qc", "sif",
    "auxiliary", "QA", "lowess_normalized_smoothed", "BioSizing",
    "segmentation", "omf", "sv", "control", "tr", "alleleSpecificCN",
    "pairedcn", "segmented", "somatic", "DGE", "Tag", "bisulfite",
    "indel", "portion", "sample", "cqcf", "follow_up", "aliquot",
    "analyte", "protocol", "diagnostic_slides", "slide", "FIRMA",
    "gene", "patient", "nte", "radiation", "drug", "B_Allele_Freq",
    "Delta_B_Allele_Freq", "Genotypes", "LOH", "Normal_LogR",
    "Paired_LogR", "seg", "segnormal", "Unpaired_LogR", "MSI", "hpv",
    "cov"
]

DATA_FORMATS = [
    "TXT", "VCF", "SVS", "idat", "CEL", "XML", "WIG",
    "PDF", "TIF", "TSV", "FSA", "SIF", "JPG", "PNG",
    "dat", "Biotab", "FA", "TR", "MAF", "BED", "DGE-Tag",
    "HTML", "MAGE-Tab", "GAF"
]

EXPERIMENTAL_STRATEGIES = [
    "Genotyping array", "RNA-Seq", "Methylation array", "DNA-Seq",
    "CGH array", "miRNA-Seq", "Protein expression array",
    "Gene expression array", "MSI-Mono-Dinucleotide Assay", "miRNA expression array",
    "WXS", "WGS", "Exon array", "Total RNA-Seq", "Mixed strategies",
    "Capillary sequencing", "Bisulfite-Seq"
]

PLATFORMS = [
    "Affymetrix SNP Array 6.0", "Illumina HiSeq", "Illumina GA",
    "Hospital Record", "Illumina Human Methylation 450",
    "MDA_RPPA_Core", "BCR Record", "HG-CGH-415K_G4124A",
    "Illumina Human Methylation 27", "HG-CGH-244A", "ABI capillary sequencer",
    "AgilentG4502A_07_3", "CGH-1x1M_G4447A", "HT_HG-U133A",
    "Illumina Human 1M Duo", "H-miRNA_8x15Kv2", "Illumina HumanHap550",
    "H-miRNA_8x15K", "AgilentG4502A_07_2", "HuEx-1_0-st-v2",
    "HG-U133_Plus_2", "Illumina DNA Methylation OMA003 CPI",
    "Illumina DNA Methylation OMA002 CPI", "AgilentG4502A_07_1", "ABI SOLiD",
    "Mixed platforms"
]

PROJECTS = ["ACC", "BLCA", "BRCA", "CESC", "CHOL", "CNTL", "COAD",
            "DLBC", "ESCA", "FPPP", "GBM", "HNSC", "KICH", "KIRC",
            "KIRP", "LAML", "LCML", "LGG", "LIHC", "LUAD", "LUSC",
            "MESO", "MISC", "OV", "PAAD", "PCPG", "PRAD", "READ",
            "SARC", "SKCM", "STAD", "TGCT", "THCA", "THYM", "UCEC",
            "UCS", "UVM"]


def idempotent_insert(driver, label, name, session):
        node = driver.node_lookup_one(label=label,
                                      property_matches={"name": name},
                                      session=session)
        if not node:
            node = driver.node_insert(PsqlNode(node_id=str(uuid.uuid4()),
                                               label=label,
                                               properties={"name": name}),
                                      session=session)
        return node


def insert_classification_nodes(driver):
    with driver.session_scope() as session:
        for data_type, subtypes in DATA_TYPES.iteritems():
            type_node = idempotent_insert(driver, "data_type",
                                          data_type, session)
            for subtype in subtypes:
                subtype_node = idempotent_insert(driver, "data_subtype",
                                                 subtype, session)
                edge = driver.edge_lookup_one(src_id=subtype_node.node_id,
                                              dst_id=type_node.node_id,
                                              label="member_of",
                                              session=session)
                if not edge:
                    edge = PsqlEdge(src_id=subtype_node.node_id,
                                    dst_id=type_node.node_id,
                                    label="member_of")
                    driver.edge_insert(edge, session=session)
        for tag in TAGS:
            idempotent_insert(driver, "tag", tag, session)
        for platform in PLATFORMS:
            idempotent_insert(driver, "platform", platform, session)
        for strat in EXPERIMENTAL_STRATEGIES:
            idempotent_insert(driver, "experimental_strategy", strat, session)
        for format in DATA_FORMATS:
            idempotent_insert(driver, "data_format", format, session)
        for project in PROJECTS:
            idempotent_insert(driver, "project", project, session)


def create_prelude_nodes(driver):
    import_center_codes(driver, os.path.join(PKG_DIR, "centerCode.csv"))
    import_tissue_source_site_codes(driver, os.path.join(PKG_DIR, "tissueSourceSite.csv"))
    insert_classification_nodes(driver)
