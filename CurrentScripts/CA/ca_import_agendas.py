#!/usr/bin/env python2.7
# -*- coding: utf8 -*-
'''
File: ca_import_agenda.py
Author: Nick Russo
Description:
- Grabs the California Legislative Agendas for database population
Sources:
- capublic database on transcription.digitaldemocracy.org
'''

from ca_hearing_parser import *
from Utils.Hearing_Manager import *
from Utils.Database_Connection import *



def main():
    with connect_to_capublic() as capublic:
        with connect() as dddb:
            cur_date = dt.datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d')
            logger = create_logger()
            parser = CaHearingsParser(dddb, capublic, cur_date, logger)
            agendas = parser.get_formatted_agendas()
            hearing_manager = Hearings_Manager(dddb, "CA", logger)
            hearing_manager.import_hearings(agendas, cur_date)
            hearing_manager.log()

if __name__ == '__main__':
    main()
