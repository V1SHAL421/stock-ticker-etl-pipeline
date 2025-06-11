"""The entry point for the Glue job to clean raw S3 data and load into cleaned zone of S3"""
import sys

from application.run_raw_to_cleaned import run_raw_to_cleaned_pipeline


def main(argv=None):
    if argv is None:
        argv = sys.argv
    run_raw_to_cleaned_pipeline()

if __name__ == "__main__":
    sys.exit(main(sys.argv))