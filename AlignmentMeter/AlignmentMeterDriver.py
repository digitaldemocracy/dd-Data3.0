#!/usr/bin/env python3.4


import ScrapeSupportFixed
import InsertAlignments
import AlignmentMeter
from Utils.Generic_Utils import *

def main():
    logger = create_logger()
    print("ScrapeSupport Started . . .\n")
    ScrapeSupportFixed.main(logger)
    print('Done.\n')
    print('Starting InsertAlignments . . .\n')
    InsertAlignments.main(logger)
    print("Done\n")
    print("Starting AlignmentMeter . . .\n")
    AlignmentMeter.main(logger)
    print('Finished\n')
    
    
if __name__ == '__main__':
    main()
