#!/usr/bin/python3
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


from ca_bill_parser import CaBillParser
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect_to_capublic, connect
from Utils.Bill_Insertion_Manager import BillInsertionManager



def main():
    with connect() as dd_cursor:
        with connect_to_capublic() as ca_public:

            logger = create_logger()

            bill_manager = BillInsertionManager(dd_cursor, logger, 'CA')
            bill_parser = CaBillParser(ca_public, dd_cursor, logger)

            vote_list = bill_parser.get_summary_votes()
            bill_manager.add_votes_db(vote_list)

            vote_detail_list = bill_parser.get_detail_votes(bill_manager)
            bill_manager.add_vote_details_db(vote_detail_list)

            bill_manager.log()


if __name__ == "__main__":
    main()
