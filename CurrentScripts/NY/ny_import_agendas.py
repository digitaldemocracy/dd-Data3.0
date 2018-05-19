#!/usr/bin/python3

import sys
from datetime import datetime
from Utils.Database_Connection import connect
from Utils.Generic_Utils import create_logger
from Utils.Generic_MySQL import get_session_year
from NY.ny_hearing_parser import NYHearingParser
from Utils.Hearing_Manager import Hearings_Manager


def main():
    with connect() as dddb:
        logger = create_logger()
        parser = NYHearingParser(get_session_year(dddb, "NY", logger))
        hearings = parser.get_hearings()
        hearing_manager = Hearings_Manager(dddb, "NY", logger)
        hearing_manager.import_hearings(hearings, datetime.today().date())
        hearing_manager.log()

if __name__ == '__main__':
    main()