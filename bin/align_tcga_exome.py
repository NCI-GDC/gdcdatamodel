from zug.binutils import zug_wrap
from zug.harmonize.tcga_exome_aligner import TCGAExomeAligner


def main():
    with zug_wrap():
        aligner = TCGAExomeAligner()
        aligner.align()

if __name__ == "__main__":
    main()
