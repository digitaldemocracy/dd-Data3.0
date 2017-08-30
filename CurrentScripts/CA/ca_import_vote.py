#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
File: Vote_Extract.py
Authored By: Daniel Mangin
Modified By: Matt Versaggi, Andrew Rose
Date: 6/11/2015
Last Modified: 8/7/2017

Description:
- Gets vote data from capublic.bill_summary_vote into DDDB.BillVoteSummary
  and capublic.bill_detail_vote into DDDB.BillVoteDetail
- Used in daily update of DDDB

Sources:
  - Leginfo (capublic)
    - Pubinfo_2015.zip
    - Pubinfo_Mon.zip
    - Pubinfo_Tue.zip
    - Pubinfo_Wed.zip
    - Pubinfo_Thu.zip
    - Pubinfo_Fri.zip
    - Pubinfo_Sat.zip

  -capublic
    - bill_summary_vote_tbl
    - bill_detail_vote_tbl

Populates:
  - BillVoteSummary (bid, mid, cid, VoteDate, ayes, naes, abstain, result)
  - BillVoteDetail (pid, voteId, result, state)
"""

import sys
import json
import MySQLdb
import traceback
from Models.Vote import *
from ca_bill_parser import *
from Constants.Bills_Queries import *
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.Bill_Insertion_Manager import *

reload(sys)
sys.setdefaultencoding('utf8')


def main():
    with connect() as dd_cursor:
        logger = create_logger()

        bill_manager = BillInsertionManager(dd_cursor, logger, 'CA')
        bill_parser = CaBillParser(logger=logger)

        vote_list = bill_parser.get_summary_votes()
        bill_manager.add_votes_db(vote_list)

        vote_detail_list = bill_parser.get_detail_votes(bill_manager)
        bill_manager.add_vote_details_db(vote_detail_list)

        bill_manager.log()


if __name__ == "__main__":
    main()
