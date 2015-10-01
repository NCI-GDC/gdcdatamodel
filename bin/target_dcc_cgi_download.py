#!/usr/bin/env python

from argparse import ArgumentParser
from zug.datamodel.target.dcc_cgi.target_dcc_cgi_sync import TargetDCCCGIDownloader

def main():
    tdc_dl = TargetDCCCGIDownloader()
    tdc_dl.process_all_work()

if __name__ == "__main__":
    main()
