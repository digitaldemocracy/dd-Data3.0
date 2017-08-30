#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: TX_import_lobbyists.py
Author: Nick Russo
Maintained: Nick Russo
Date: 4/3/2017
Last Modified: 4/3/17

Description:
  - Imports TX lobbyist data from files from TX lobbyist registration website.

Populates:
  - Person
    - (first, last)
  - Lobbyist
    - (pid, filer_id, state)
  - LobbyingFirm
    - (filer_naml)
  - LobbyingFirmState
    - (filer_id, rpt_date, ls_beg_yr, ls_end_yr, filer_naml, state)
  - LobbyistEmployment (X not populated X)
    - (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyistDirectEmployment
    - (pid, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyingContracts
    - (filer_id, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyistEmployer
    - (filer_id, oid, state)
  - Organizations
    - (name, city, stateHeadquartered, type)

Source:
TX_LOBBYIST_URL = 'https://www.ethics.state.tx.us/tedd/2017LobbyistGroupByClient.nopag.xlsx'
'''
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from tx_lobbyist_parser import TxLobbyistParser
from Utils.Lobbyist_Insertion_Manager import LobbyistInsertionManager


def main():
    with connect() as dddb:
        logger = create_logger()
        parser = TxLobbyistParser()
        manager = LobbyistInsertionManager(dddb, "TX", logger)
        lobbyist = parser.parse()
        manager.import_lobbyists(lobbyist)
        manager.log()


if __name__ == '__main__':
    main()

