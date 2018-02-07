#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

import sys
from datetime import datetime
from Utils.Database_Connection import connect
from Utils.Generic_Utils import create_logger
from Utils.Generic_MySQL import get_session_year
from NY.ny_hearing_parser import NYHearingParser
from Utils.Hearing_Manager import Hearings_Manager

reload(sys)
sys.setdefaultencoding('utf-8')

def main():
<<<<<<< Updated upstream
    with connect() as dddb:

        #set all hearing agendas prior to today to inactive
        update_hearing_agenda(dddb)

        #get current session year
        year = get_session_year(dddb)

        #scrape assembly committee agendas
        assembly_comm_hearings = get_assembly_comm_hearings()
        for hearing in assembly_comm_hearings:
            cid = get_cid(dddb, 'Assembly', hearing['name'], year)
            if cid is not None:
                hid = insert_hearing(dddb, hearing['date'], year, cid)
            
                if hid is not None and cid is not None:
                    insert_comm_hearing(dddb, cid, hid)
                bills = scrape_hearing_agenda(dddb, hearing['url'], 'Assembly')
                if hid is not None and len(bills) > 0:
                    for bid in bills:
                        insert_hearing_agenda(dddb, hid, bid)

        #scrape senate committee agendas
        # senate_comm_hearings = get_senate_comm_hearings()
        # for hearing in senate_comm_hearings:
        #     cid = get_cid(dddb, 'Senate', hearing['name'], year)
        #     if cid is not None:
        #         hid = insert_hearing(dddb, hearing['date'], year, cid)
        #
        #         if hid is not None and cid is not None:
        #             insert_comm_hearing(dddb, cid, hid)
        #
        #         bills = scrape_senate_agenda(dddb, hearing['url'])
        #         if hid is not None and len(bills) > 0:
        #             for bid in bills:
        #                 insert_hearing_agenda(dddb, hid, bid)

    LOG = {'tables': [{'state': 'NY', 'name': 'Hearing', 'inserted':I_H, 'updated': 0, 'deleted': 0},
      {'state': 'NY', 'name': 'CommitteeHearings', 'inserted':I_CH, 'updated': 0, 'deleted': 0},
      {'state': 'NY', 'name': 'HearingAgenda', 'inserted':I_HA, 'updated': U_HA, 'deleted': 0}]}
    sys.stdout.write(json.dumps(LOG))
    logger.info(LOG)
=======
    with connect("live") as dddb:
        logger = create_logger()
        parser = NYHearingParser(get_session_year(dddb, "NY", logger))
        hearings = parser.get_hearings()
        hearing_manager = Hearings_Manager(dddb, "NY", logger)
        hearing_manager.import_hearings(hearings, datetime.today().date())
        hearing_manager.log()
>>>>>>> Stashed changes

if __name__ == '__main__':
    main()