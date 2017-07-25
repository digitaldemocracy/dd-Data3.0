'''
ca_import_committees
Author: Nick Russo
Purpose: Web scrapes assembly and senate websites to get committee names and committee website links.
         Then web scrapes each committee website for committee member information.
'''
from ca_committee_parser import *
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from Utils.Generic_MySQL import get_session_year
from Utils.Committee_Insertion_Manager import CommitteeInsertionManager


def main():
    with connect() as dddb:
        logger = create_logger()
        print("Getting session year...")
        session_year = get_session_year(dddb, "CA", logger)
        committee_insertion_manager = CommitteeInsertionManager(dddb, "CA", session_year, logger)
        parser = CaCommitteeParser(session_year)
        print("Getting Committee List...")
        committees = parser.get_committee_list()
        print("Starting import...")
        committee_insertion_manager.import_committees(committees)
        print("Import finished")
        committee_insertion_manager.log()


if __name__ == '__main__':
        main()
