from argparse import ArgumentParser
from zug.binutils import zug_wrap
from zug.harmonize.target_wgs_aligner import TARGETWGSAligner


def main():
    parser = ArgumentParser()
    parser.add_argument("--file-id", help="Force this id to be aligned")
    args = parser.parse_args()
    with zug_wrap():
        aligner = TARGETWGSAligner(
            force_input_id=args.file_id
        )
        aligner.go()

if __name__ == "__main__":
    main()
