CLASSIFICATION = {
  "mirna-seq": {
    "l3": {
      ".+(isoform|mirna).quantification.txt": {
        "platform": "Illumina HiSeq",
        "data_format": "TXT",
        "data_subtype": "miRNA quantification",
        "data_type": "Gene expression",
        "experimental_strategy": "miRNA-Seq"
      }
    }
  },
  "bisulfite-seq": {
    "l3": {
      ".+fractional.bw": {
        "platform": "Illumina HiSeq",
        "data_format": "BW",
        "data_subtype": "Methylation beta value",
        "data_type": "DNA methylation",
        "experimental_strategy": "WGBS"
      }
    }
  },
  "gwas": {
    ".+hh550.+txt": {
      "platform": "Illumina HumanHap550",
      "data_format": "TXT",
      "data_subtype": "Genotypes",
      "data_type": "Simple nucleotide variation",
      "experimental_strategy": "Genotyping array"
    }
  },
  "wxs": {
    "l4": {
      ".+txt": {
        "platform": "Complete Genomics",
        "data_format": "TXT",
        "data_subtype": "Simple nucleotide variation",
        "data_type": "Simple nucleotide variation",
        "experimental_strategy": "DNA-Seq"
      }
    },
    "l3": {
      "copy_number": {
        ".+sif": {
          "data_type": "Copy number variation",
          "data_format": "SIF",
          "platform": "Complete Genomics",
          "tag": "summary",
          "data_subtype": "Copy number variation",
          "experimental_strategy": "DNA-Seq"
        },
        ".+copy_number_segments.zip": {
          "platform": "Complete Genomics",
          "data_format": "ZIP",
          "data_subtype": "Copy number segmentation",
          "data_type": "Copy number variation",
          "experimental_strategy": "DNA-Seq"
        },
        ".+seg.txt": {
          "platform": "Complete Genomics",
          "data_format": "TXT",
          "data_subtype": "Copy number segmentation",
          "data_type": "Copy number variation",
          "experimental_strategy": "DNA-Seq"
        },
        "mss": {
          ".+zip": {
            "platform": "Complete Genomics",
            "data_format": "PNG",
            "data_subtype": "LOH",
            "data_type": "Copy number variation",
            "experimental_strategy": "DNA-Seq"
          },
          ".+txt": {
            "platform": "Complete Genomics",
            "data_format": "TXT",
            "data_subtype": "LOH",
            "data_type": "Copy number variation",
            "experimental_strategy": "DNA-Seq"
          }
        }
      },
      "mutation": {
        "bcm": {
          ".+mafplus.xlsx": {
            "platform": "Complete Genomics",
            "data_format": "xlsx",
            "data_subtype": "Simple somatic mutation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          },
          ".+mafplus.txt": {
            "platform": "Complete Genomics",
            "data_format": "xlsx",
            "data_subtype": "Simple somatic mutation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          },
          ".+txt": {
            "platform": "Complete Genomics",
            "data_format": "TXT",
            "data_subtype": "Simple nucleotide variation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          }
        },
        ".+mafplus.xlsx": {
          "platform": "Complete Genomics",
          "data_format": "xlsx",
          "data_subtype": "Simple somatic mutation",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "DNA-Seq"
        },
        "stjude": {
          ".+somatic.maf.txt": {
            "platform": "Complete Genomics",
            "data_format": "MAF",
            "data_subtype": "Simple somatic mutation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          }
        },
        "broad": {
          "target_nbl_wxs_somatic_calls.maf.txt": {
            "platform": "Complete Genomics",
            "data_format": "MAF",
            "data_subtype": "Simple somatic mutation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          },
          "nb170_200lines.txt": {
            "platform": "Complete Genomics",
            "data_format": "TXT",
            "data_subtype": "Simple nucleotide variation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          },
          ".+vcf(.gz)?": {
            "platform": "Complete Genomics",
            "data_format": "VCF",
            "data_subtype": "Simple nucleotide variation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          }
        },
        "nci": {
          ".+somatic.exonic.bcmmaf.txt": {
            "platform": "Complete Genomics",
            "data_format": "TXT",
            "data_subtype": "Simple somatic mutation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          }
        }
      }
    }
  },
  "targeted_capture_sequencing": {
    ".+": {
      "platform": "ABI capillary sequencer",
      "data_format": "TXT",
      "data_subtype": "ABI sequence trace",
      "data_type": "Other",
      "experimental_strategy": "Capillary sequencing"
    },
    "l3": {
      ".+maf.txt": {
        "platform": "Illumina HiSeq",
        "data_format": "MAF",
        "data_subtype": "Simple nucleotide variation",
        "data_type": "Simple nucleotide variation",
        "experimental_strategy": "DNA-Seq"
      },
      "non-wt": {
        ".+maf.txt": {
          "platform": "Illumina HiSeq",
          "data_format": "MAF",
          "data_subtype": "Simple nucleotide variation",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "DNA-Seq"
        }
      },
      ".+vcf": {
        "platform": "Illumina HiSeq",
        "data_format": "VCF",
        "data_subtype": "Simple nucleotide variation",
        "data_type": "Simple nucleotide variation",
        "experimental_strategy": "DNA-Seq"
      }
    }
  },
  "mrna-seq": {
    "l3": {
      "expression": {
        ".+gene.quantification.txt": {
          "platform": "Illumina HiSeq",
          "data_format": "TXT",
          "data_subtype": "Gene expression quantification",
          "data_type": "Gene expression",
          "experimental_strategy": "RNA-Seq"
        },
        ".+exon.quantification.txt": {
          "platform": "Illumina HiSeq",
          "data_format": "TXT",
          "data_subtype": "Exon quantification",
          "data_type": "Gene expression",
          "experimental_strategy": "RNA-Seq"
        },
        ".+spljxn.quantification.txt": {
          "platform": "Illumina HiSeq",
          "data_format": "TXT",
          "data_subtype": "Exon junction quantification",
          "data_type": "Gene expression",
          "experimental_strategy": "RNA-Seq"
        },
        ".+isoform.quantification.txt": {
          "platform": "Illumina HiSeq",
          "data_format": "TXT",
          "data_subtype": "Isoform expression quantification",
          "data_type": "Gene expression",
          "experimental_strategy": "RNA-Seq"
        }
      },
      "mutation": {
        ".+maf.txt": {
          "platform": "Illumina HiSeq",
          "data_format": "MAF",
          "data_subtype": "Simple nucleotide variation",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "RNA-Seq"
        },
        ".+vcf": {
          "platform": "Illumina HiSeq",
          "data_format": "VCF",
          "data_subtype": "Simple nucleotide variation",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "RNA-Seq"
        }
      },
      "structural": {
        ".+vcf": {
          "platform": "Illumina HiSeq",
          "data_format": "VCF",
          "data_subtype": "Structural variation",
          "data_type": "Structural rearrangement",
          "experimental_strategy": "RNA-Seq"
        }
      }
    }
  },
  "copy_number_array": {
    "l4": {
      ".+pdf": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "PDF",
        "data_subtype": "Copy number summary",
        "data_type": "Copy number variation",
        "experimental_strategy": "Genotyping array"
      },
      ".+txt": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "TXT",
        "data_subtype": "Copy number summary",
        "data_type": "Copy number variation",
        "experimental_strategy": "Genotyping array"
      }
    },
    "l2": {
      "logr": {
        ".+txt": {
          "platform": "Affymetrix SNP Array 6.0",
          "data_format": "TXT",
          "data_subtype": "Copy number estimate",
          "data_type": "Copy number variation",
          "experimental_strategy": "Genotyping array"
        }
      },
      "genotype": {
        ".+txt": {
          "platform": "Affymetrix SNP Array 6.0",
          "data_format": "TXT",
          "data_subtype": "Genotypes",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "Genotyping array"
        }
      },
      ".+-(n|t)-d.txt": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "TXT",
        "data_subtype": "Genotypes",
        "data_type": "Simple nucleotide variation",
        "experimental_strategy": "Genotyping array"
      },
      ".+chp": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "CHP",
        "data_subtype": "Probeset summary",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Genotyping array"
      },
      "chp": {
        ".+birdseed-v2.+txt": {
          "platform": "Affymetrix SNP Array 6.0",
          "data_format": "TXT",
          "data_subtype": "Genotypes",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "Genotyping array"
        },
        ".+chp": {
          "platform": "Affymetrix SNP Array 6.0",
          "data_format": "CHP",
          "data_subtype": "Probeset summary",
          "data_type": "Raw microarray data",
          "experimental_strategy": "Genotyping array"
        }
      },
      ".+(knsp|ksty).+txt": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "TXT",
        "data_subtype": "Genotypes",
        "data_type": "Simple nucleotide variation",
        "experimental_strategy": "Genotyping array"
      },
      "cnmz": {
        ".+cnmz.txt": {
          "platform": "Affymetrix SNP Array 6.0",
          "data_format": "TXT",
          "data_subtype": "Probeset call",
          "data_type": "Copy number variation",
          "experimental_strategy": "Genotyping array"
        }
      },
      "txt": {
        ".+txt": {
          "platform": "Affymetrix SNP Array 6.0",
          "data_format": "TXT",
          "data_subtype": "Genotypes",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "Genotyping array"
        }
      },
      "signal": {
        ".+txt": {
          "platform": "Affymetrix SNP Array 6.0",
          "data_format": "TXT",
          "data_subtype": "Copy number estimate",
          "data_type": "Copy number variation",
          "experimental_strategy": "Genotyping array"
        }
      },
      ".+cn_level2_log2.+txt": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "TXT",
        "data_subtype": "Copy number estimate",
        "data_type": "Copy number variation",
        "experimental_strategy": "Genotyping array"
      },
      "target-\\w{2}-\\w{6}-\\w{3}-\\w{3}.txt": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "TXT",
        "data_subtype": "Genotypes",
        "data_type": "Simple nucleotide variation",
        "experimental_strategy": "Genotyping array"
      }
    },
    "l3": {
      ".+txt": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "TXT",
        "data_subtype": "Copy number segmentation",
        "data_type": "Copy number variation",
        "experimental_strategy": "Genotyping array"
      }
    },
    "l1": {
      ".+cel": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "CEL",
        "data_subtype": "Raw intensities",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Genotyping array"
      },
      ".+idat": {
        "platform": "Affymetrix SNP Array 6.0",
        "data_format": "idat",
        "data_subtype": "Raw intensities",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Genotyping array"
      }
    }
  },
  "clinical": {
    ".+xlsx": {
      "platform": "Hospital Record",
      "data_subtype": "Clinical data",
      "data_type": "Clinical",
      "data_format": "xlsx"
    },
    "harmonized": {
      ".+harmonized.(xls|xlsx)": {
        "platform": "Hospital Record",
        "tag": "harmonized",
        "data_subtype": "Clinical data",
        "data_type": "Clinical",
        "data_format": "xlsx"
      }
    }
  },
  "mirna_pcr": {
    "l2": {
      ".+txt": {
        "data_type": "Gene expression",
        "data_format": "TXT",
        "platform": "MegaPlex TaqMan",
        "tag": "summary",
        "data_subtype": "miRNA quantification",
        "experimental_strategy": "RT-PCR"
      }
    },
    "l1": {
      ".+(xls|xlsx)": {
        "platform": "MegaPlex TaqMan",
        "data_format": "xlsx",
        "data_subtype": "miRNA quantification",
        "data_type": "Gene expression",
        "experimental_strategy": "RT-PCR"
      },
      ".+txt": {
        "platform": "MegaPlex TaqMan",
        "data_format": "TXT",
        "data_subtype": "miRNA quantification",
        "data_type": "Gene expression",
        "experimental_strategy": "RT-PCR"
      }
    }
  },
  "wgs": {
    "l4": {
      "cgi": {
        "circos": {
          "somaticcircos.+png": {
            "platform": "Complete Genomics",
            "data_format": "PNG",
            "data_subtype": "Structural variation",
            "data_type": "Structural rearrangement",
            "experimental_strategy": "DNA-Seq"
          }
        }
      }
    },
    "l3": {
      "copy_number": {
        "cgi": {
          ".+tsv": {
            "platform": "Complete Genomics",
            "data_format": "TSV",
            "data_subtype": "Copy number segmentation",
            "data_type": "Copy number variation",
            "experimental_strategy": "DNA-Seq"
          }
        }
      },
      "mutation": {
        "cgi": {
          "fullmafsvcfs": {
            ".+maf.txt": {
              "platform": "Complete Genomics",
              "data_format": "MAF",
              "data_subtype": "Simple somatic mutation",
              "data_type": "Simple nucleotide variation",
              "experimental_strategy": "DNA-Seq"
            },
            ".+vcf.bz2": {
              "platform": "Complete Genomics",
              "data_format": "VCF",
              "data_subtype": "Simple nucleotide variation",
              "data_type": "Simple nucleotide variation",
              "experimental_strategy": "DNA-Seq"
            },
            "illumina": {
              ".+vcf.bz2": {
                "platform": "Illumina HiSeq",
                "data_format": "VCF",
                "data_subtype": "Simple nucleotide variation",
                "data_type": "Simple nucleotide variation",
                "experimental_strategy": "DNA-Seq"
              }
            }
          },
          "somaticfilteredmafs": {
            "somatic.+maf.txt": {
              "platform": "Complete Genomics",
              "data_format": "MAF",
              "data_subtype": "Simple somatic mutation",
              "data_type": "Simple nucleotide variation",
              "experimental_strategy": "DNA-Seq"
            }
          },
          "analysis": {
            ".*variant.+tsv": {
              "platform": "Complete Genomics",
              "data_format": "TSV",
              "data_subtype": "Simple nucleotide variation",
              "data_type": "Simple nucleotide variation",
              "experimental_strategy": "DNA-Seq"
            },
            ".+somatic.+xlsx": {
              "platform": "Complete Genomics",
              "data_format": "xlsx",
              "data_subtype": "Simple somatic mutation",
              "data_type": "Simple nucleotide variation",
              "experimental_strategy": "DNA-Seq"
            }
          }
        },
        "bcca": {
          ".+maf.txt": {
            "platform": "Complete Genomics",
            "data_format": "MAF",
            "data_subtype": "Simple nucleotide variation",
            "data_type": "Simple nucleotide variation",
            "experimental_strategy": "DNA-Seq"
          }
        },
        ".+dna_(tumor|normal).maf.txt": {
          "platform": "Complete Genomics",
          "data_format": "MAF",
          "data_subtype": "Simple nucleotide variation",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "DNA-Seq"
        },
        ".+somatic.maf.txt": {
          "platform": "Complete Genomics",
          "data_format": "MAF",
          "data_subtype": "Simple somatic mutation",
          "data_type": "Simple nucleotide variation",
          "experimental_strategy": "DNA-Seq"
        }
      },
      "structural": {
        "cgi": {
          "junctions": {
            "concatenatedjunctionfile.+txt": {
              "platform": "Complete Genomics",
              "data_format": "TXT",
              "data_subtype": "Structual junction",
              "data_type": "Structural rearrangement",
              "experimental_strategy": "DNA-Seq"
            },
            "concatenatedjunctionfile.+xlsx": {
              "platform": "Complete Genomics",
              "data_format": "xlsx",
              "data_subtype": "Structual junction",
              "data_type": "Structural rearrangement",
              "experimental_strategy": "DNA-Seq"
            }
          }
        },
        ".+vcf": {
          "platform": "Complete Genomics",
          "data_format": "VCF",
          "data_subtype": "Structural variation",
          "data_type": "Structural rearrangement",
          "experimental_strategy": "DNA-Seq"
        }
      }
    }
  },
  "gene_expression_array": {
    "l4": {
      ".+xlsx": {
        "platform": "Affymetrix U133 Plus 2",
        "data_format": "xlsx",
        "data_subtype": "Gene expression summary",
        "data_type": "Gene expression",
        "experimental_strategy": "Gene expression array"
      },
      ".+pdf": {
        "platform": "Affymetrix U133 Plus 2",
        "data_format": "PDF",
        "data_subtype": "Gene expression summary",
        "data_type": "Gene expression",
        "experimental_strategy": "Gene expression array"
      }
    },
    "l2": {
      ".+txt": {
        "platform": "Affymetrix U133 Plus 2",
        "data_format": "TXT",
        "data_subtype": "Normalized intensities",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Gene expression array"
      },
      "chp": {
        ".+chp": {
          "platform": "Affymetrix U133 Plus 2",
          "data_format": "CHP",
          "data_subtype": "Probeset summary",
          "data_type": "Raw microarray data",
          "experimental_strategy": "Gene expression array"
        }
      },
      "raw": {
        ".+exon.+txt": {
          "platform": "Affymetrix U133 Plus 2",
          "data_format": "TXT",
          "data_subtype": "Normalized intensities",
          "data_type": "Raw microarray data",
          "experimental_strategy": "Gene expression array"
        }
      },
      "ber": {
        ".+exon.+batcheffectremoved.+txt": {
          "data_type": "Raw microarray data",
          "data_format": "TXT",
          "platform": "Affymetrix U133 Plus 2",
          "tag": "batch_effect_removed",
          "data_subtype": "Normalized intensities",
          "experimental_strategy": "Gene expression array"
        }
      },
      "exon.+txt": {
        "platform": "Affymetrix U133 Plus 2",
        "data_format": "TXT",
        "data_subtype": "Exon quantification",
        "data_type": "Gene expression",
        "experimental_strategy": "Gene expression array"
      },
      ".+gct": {
        "platform": "Affymetrix U133 Plus 2",
        "data_format": "GCT",
        "data_subtype": "Normalized intensities",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Gene expression array"
      }
    },
    "l3": {
      ".+gct": {
        "platform": "Affymetrix U133 Plus 2",
        "data_format": "GCT",
        "data_subtype": "Gene expression quantification",
        "data_type": "Gene expression",
        "experimental_strategy": "Gene expression array"
      },
      "gene": {
        "core": {
          ".+core_gene.+txt": {
            "platform": "Affymetrix U133 Plus 2",
            "data_format": "TXT",
            "data_subtype": "Gene expression quantification",
            "data_type": "Gene expression",
            "experimental_strategy": "Gene expression array"
          }
        },
        "extended": {
          ".+extended_gene.+txt": {
            "platform": "Affymetrix U133 Plus 2",
            "data_format": "TXT",
            "data_subtype": "Gene expression quantification",
            "data_type": "Gene expression",
            "experimental_strategy": "Gene expression array"
          }
        },
        "full": {
          ".+full_gene.+txt": {
            "platform": "Affymetrix U133 Plus 2",
            "data_format": "TXT",
            "data_subtype": "Gene expression quantification",
            "data_type": "Gene expression",
            "experimental_strategy": "Gene expression array"
          }
        }
      },
      "transcript": {
        "core": {
          ".+core_transcript.+txt": {
            "platform": "Affymetrix U133 Plus 2",
            "data_format": "TXT",
            "data_subtype": "Normalized intensities",
            "data_type": "Raw microarray data",
            "experimental_strategy": "Gene expression array"
          }
        },
        "extended": {
          ".+extended_transcript.+txt": {
            "platform": "Affymetrix U133 Plus 2",
            "data_format": "TXT",
            "data_subtype": "Normalized intensities",
            "data_type": "Raw microarray data",
            "experimental_strategy": "Gene expression array"
          }
        },
        "full": {
          ".+full_transcript.+txt": {
            "platform": "Affymetrix U133 Plus 2",
            "data_format": "TXT",
            "data_subtype": "Normalized intensities",
            "data_type": "Raw microarray data",
            "experimental_strategy": "Gene expression array"
          }
        }
      },
      ".+txt": {
        "platform": "Affymetrix U133 Plus 2",
        "data_format": "TXT",
        "data_subtype": "Gene expression quantification",
        "data_type": "Gene expression",
        "experimental_strategy": "Gene expression array"
      }
    },
    "l1": {
      ".+cel": {
        "platform": "Affymetrix U133 Plus 2",
        "data_format": "CEL",
        "data_subtype": "Raw intensities",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Gene expression array"
      }
    }
  },
  "kinome": {
    ".+kinomelinkingtable.+txt": {
      "platform": "ABI capillary sequencer",
      "data_format": "TXT",
      "data_subtype": "ABI sequence trace",
      "data_type": "Other",
      "experimental_strategy": "Capillary sequencing"
    }
  },
  "methylation_array": {
    "l2": {
      ".+txt": {
        "platform": "Illumina Human Methylation 450",
        "data_format": "TXT",
        "data_subtype": "Intensities",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Methylation array"
      }
    },
    "l3": {
      ".+txt": {
        "platform": "Illumina Human Methylation 450",
        "data_format": "TXT",
        "data_subtype": "Methylation beta value",
        "data_type": "DNA methylation",
        "experimental_strategy": "Methylation array"
      }
    },
    "l1": {
      ".+idat": {
        "platform": "Illumina Human Methylation 450",
        "data_format": "idat",
        "data_subtype": "Raw intensities",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Methylation array"
      },
      ".+pair": {
        "platform": "Illumina Human Methylation 450",
        "data_format": "PAIR",
        "data_subtype": "Raw intensities",
        "data_type": "Raw microarray data",
        "experimental_strategy": "Methylation array"
      }
    }
  }
}


IGNORE = [
    "md5checksum_file",
    "version",
    "readme.*",
    "manifest.+",
    ".+readme.+pdf",
    ".+doc",
    ".+(bpm|egt)",
    "wt cde final.xlsx",
    "target_allp1_kinome_listofsequencedgenes.xls",
    "unique.txt",
    ".+docx",
    "nb170_cols.txt",
    "nb170_samps.txt",
    "nbl_wxs_normal_samps_20130805.txt",
    "nbl_wxs_samps_20130730.txt",
]
