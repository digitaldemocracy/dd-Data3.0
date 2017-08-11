#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
File: ny_import_contributions.py
Author: James Ly
Last Maintained: Andrew Rose
Last Updated: 08/10/2017

Description:
 - imports contributions for NY from followthemoney.org

Tables affected:
 - Organizations
 - Contribution
"""

import re
import sys
import time
import json
import MySQLdb
import requests
import traceback
from datetime import datetime
from bs4 import BeautifulSoup
from Utils.Generic_Utils import *
#from Utils.Generic_MySQL import *
from Utils.Database_Connection import *
from Utils.Contribution_Insertion_Manager import ContributionInsertionManager
from OpenStatesParsers.contributions_parser import ContributionParser
from Constants.Contribution_Queries import *

logger = None


def main():
    with connect() as dddb:
        contribution_parser = ContributionParser('NY', candidates_file='NY-Build/candidates.txt')
        contribution_manager = ContributionInsertionManager(dddb, logger, 'NY')

        contribution_list = contribution_parser.parse_followthemoney_contributions()

        contribution_manager.insert_contributions_db(contribution_list)
        contribution_manager.log()
    

if __name__ == '__main__':
    logger = create_logger()
    main()
