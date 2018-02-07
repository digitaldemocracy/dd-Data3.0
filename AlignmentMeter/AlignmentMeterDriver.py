#!/usr/bin/env python3.4


import ScrapeSupportFixed
import InsertAlignments
import AlignmentMeter
from Utils.Generic_Utils import *

def main():
    logger = create_logger()

    ScrapeSupportFixed.main(logger)
    InsertAlignments.main(logger)
    AlignmentMeter.main(logger)
    
    
if __name__ == '__main__':
    main()
