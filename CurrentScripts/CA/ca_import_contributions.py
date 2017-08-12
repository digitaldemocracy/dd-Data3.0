#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: insert_Contributions_CSV.py
Author: Daniel Mangin & Mandy Chan
Date: 6/11/2015
Last Updated: 8/11/2017

Description:
- Gathers Contribution Data and puts it into DDDB2015.Contributions
- Used once for the Insertion of all the Contributions
- Fills table:
  Contribution (id, pid, year, date, house, donorName, donorOrg, amount)

Sources:
- Maplight Data
  - cand_2001.csv
  - cand_2003.csv
  - cand_2005.csv
  - cand_2007.csv
  - cand_2009.csv
  - cand_2011.csv
  - cand_2013.csv
  - cand_2015.csv
"""

from Utils.Generic_Utils import *
from Utils.Generic_MySQL import *
from Utils.Database_Connection import *
from Utils.Contribution_Insertion_Manager import ContributionInsertionManager
from ca_contribution_parser import CaContributionParser

logger = None


def main():
    #global zipURL
    with connect() as dddb:
        year = get_session_year(dddb, 'CA', logger)
        contribution_parser = CaContributionParser(year)
        contribution_manager = ContributionInsertionManager(dddb, logger, 'CA')

        contribution_list = contribution_parser.get_contributions()
        contribution_manager.insert_contributions_db(contribution_list)
        contribution_manager.log()


if __name__ == "__main__":
    logger = create_logger()
    main()
