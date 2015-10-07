from argparse import ArgumentParser
from zug.binutils import zug_wrap
from zug.harmonize.target_rnaseq_aligner import TARGETRNASeqAligner


def main():
    parser = ArgumentParser()
    parser.add_argument("--file-id", help="Force this id to be aligned")
    args = parser.parse_args()
    with zug_wrap():
        aligner = TARGETRNASeqAligner(
            force_input_id=args.file_id
        )
        aligner.go()

if __name__ == "__main__":
    main()
