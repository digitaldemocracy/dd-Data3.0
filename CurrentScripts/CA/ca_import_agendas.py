#!/usr/bin/env python

'''
File: ca_agenda.py
Author: Sam Lakes
Date Created: July 27th, 2016
Last Modified: August 24th, 2016
Description:
- Grabs the California Legislative Agendas for database population
Sources:
- capublic database on transcription.digitaldemocracy.org
'''

from ca_hearing_parser import *
from Utils.Hearing_Manager import *
from Utils.Database_Connection import *

logger = None

# Global counters
H_INS = 0  # Hearings inserted
CH_INS = 0  # CommitteeHearings inserted
HA_INS = 0  # HearingAgenda inserted
HA_UPD = 0  # HearingAgenda updated





def main():
    with connect_to_capublic() as capublic:
        with connect("live") as dddb:
            print("Starting")
            cur_date = dt.datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d')
            logger = create_logger()
            print("entering parser")
            parser = CaHearingsParser(dddb, capublic, cur_date, logger)
            agendas = parser.get_formatted_agendas()
            print("done formatting")
            hearing_manager = Hearings_Manager(dddb, "CA")
            print("inserting")
            hearing_manager.import_hearings(agendas, cur_date)
            hearing_manager.log()

if __name__ == '__main__':
    logger = create_logger()
    main()