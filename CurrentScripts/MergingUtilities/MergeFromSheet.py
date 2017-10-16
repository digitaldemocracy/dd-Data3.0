#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: MergeFromSheet
Author: Nathan Philliber
Date: 3 October 2017
Last Updated: 3 October 2017

Purpose:
    - Quickly merge many people and organizations from a google
        spreadsheet.
    - Organizations are merged using 'concept organization' strategy
"""

import gspread
import argparse
import MySQLdb
import datetime
from Utils.Database_Connection import *
from Utils.Generic_Utils import *
from oauth2client.service_account import ServiceAccountCredentials
from MergingUtilities.PersonMerge import complete_merge_person as mergePerson
from MergingUtilities.OrgMerge import *

#Default authenitifcation json path
googleAuthentification = '<PATH TO JSON KEY HERE>'
#Default Google Sheets key
sheetKey = '<DEFAULT SHEET KEY HERE>'


#Sheet type variables
personType = 'Person'
organizationType = 'Organization'

logger = create_logger()

def merge_pair(dddb, sheetType, goodID, badID, failed, successful):
    """
    Calls the appropriate merge function for that sheet type.
    Merges a goodId and a badID.
    :param dddb: connection to the database
    :param sheetType: a string containing the type of sheet (person/organization)
    :param goodID: the id to be merged into
    :param badID: the id to be removed
    :param failed: dictionary failed attempts will be added to
    :param successful: dictionary successful merges will be added to
    """
    try:
        print("\tMerging    " + str(badID) + "\t into \t" + str(goodID))

        # Person Type
        if(sheetType == personType):
            mergePerson(dddb, {'good_pid': goodID, 'bad_pid': badID})

        # Organization Type
        elif(sheetType == organizationType):
            conceptOID = has_org_concept(dddb, {'oid': goodID})
            if(conceptOID == 0):
                # Add a concept org
                print("Creating new concept org for " + str(goodID))
                conceptOID = add_org_concept(dddb, {'good_oid':goodID, 'concept':get_org_name(dddb, {'oid':goodID})})
                print("\tNew concept oid: " + str(conceptOID))
            else:
                print("Already found a concept org with oid: " + str(conceptOID))

            merge_org(dddb, {'good_oid':goodID, 'bad_oid': badID, 'is_subchapter':False}, throw_exc=True)
            merge_org_concept(dddb, {'good_oid': conceptOID, 'bad_oid': badID, 'is_subchapter': False}, throw_exc=True)

        # If it makes it here, then merge was successful
        # Store the id pair in the successful dictionary
        if str(goodID) not in successful:
            successful[str(goodID)] = [goodID]
        successful[str(goodID)].append(badID)
        logger.info(format_end_log("Successful Merge", sheetType, "Merged " + str(badID) + " into " + str(goodID)))

    except:
        # Merge failed, store id pair in failed dictionary
        logger.exception(format_end_log("Failed Merge", sheetType, "Failed to merge " + str(badID) + " into " + str(goodID)))
        print("\t\t[ERROR] Failed to merge " + str(badID) + "\t into \t" + str(goodID))
        if str(goodID) not in failed:
            failed[str(goodID)] = [goodID]
        failed[str(goodID)].append(badID)

def process_row(dddb, sheet, sheetType, row, failed, successful):
    """
    Processes a row. The first cell is the goodID, every cell after that
    is a badID that is to be merged with that goodID.
    :param dddb: connection to the database
    :param sheetType: a string containing the type of sheet (person/organization)
    :param row: list representing the row in the table. Index 0 is goodID, rest are badIDs
    :param failed: dictionary failed attempts will be added to
    :param successful: dictionary successful merges will be added to
    """
    for badID in row[1:]:
        if(badID != ''):
            merge_pair(dddb, sheetType, row[0], badID, failed, successful)

def process_sheet(dddb, spreadsheet, sheet, sheetType, keepSheet, keepFailed=False, sheetHist=None):
    """
    Loops through all the rows in a sheet and calls process_row on
    each row. Clears the sheet at the end, if specified.
    :param dddb: connection to the database
    :param spreadsheet: gspread worksheet object
    :param sheet: gspread sheet object
    :param sheetType: a string containing the type of sheet (person/organization)
    :param keepSheet: a boolean, True if don't want to clear sheet, False is want to clear sheet
    :param keepFailed: a boolean, True if want to put failed attempts back into spreadsheet, False otherwise
    :param sheetHist: a gspread sheet, successful merges will be stored here
    :return: returns message to be logged
    """
    failed = {}
    successful = {}

    print("Processing Sheet: " + sheetType)
    csvFile = sheet.export().split('\r\n')
    print("\t\tRAW CSV:\n\t\t\t" + '\n\t\t\t'.join(csvFile[1:]))

    # Actually process the sheet
    for row in csvFile[1:]:
        process_row(dddb, sheet, sheetType, row.split(','), failed, successful)

    # Clear sheet if needed
    if(keepSheet is False):
        print("Clearing Sheet: " + sheetType)
        clear_sheet(sheet, keepFailed, failed)

    # Store the sheet history if needed
    if(sheetHist is not None):
        print("Storing History for Sheet: " + sheetType)
        store_history(sheetHist, successful)

    # Calculate number of failed/successful merges
    numF = 0
    numS = 0
    for row in failed:
        numF += len(failed[row]) - 1
    for row in successful:
        numS += len(successful[row]) - 1

    msg = "[" + sheetType + "][ Merged: " + str(numS) + " / " + str(numF+numS) + " ]"
    if(numF > 0):
        msg += " ( " + str(numF) + " FAILED )"
    return msg

def clear_sheet(sheet, keepFailed=False, failed=None):
    """
    Deletes all rows except the first row.
    :param sheet: the gspread sheet object to clear
    :param keepFailed: a boolean, True if want to put failed merges back into sheet, False if not
    :param failed: dictionary containing all failed merges
    """
    firstRow = sheet.row_values(1)
    sheet.clear()
    sheet.insert_row(firstRow)
    if(keepFailed and failed is not None):
        put_back_bad(sheet, failed)

def put_back_bad(sheet, failed):
    """
    Puts all failed merges back into sheet. Assumes that sheet has been cleared.
    :param sheet: gspread sheet object
    :param failed: dictionary containing all failed merges
    """
    curRow = 2
    for key in failed:
        sheet.insert_row(failed[key], curRow)
        curRow += 1

def store_history(sheet, successful):
    """
    Stores all successful merges into sheet. Appends rows to end of table.
    :param sheet: gspread sheet object
    :param successful: dictionary containging all successful merges
    """
    for key in successful:
        sheet.append_row(successful[key])

def main():
    argParser = argparse.ArgumentParser(description='Merges people and organizations from google sheet')
    argParser.add_argument('-s', '--sheet', help='The google sheet key to be processed', type=str, default=sheetKey, metavar='sheetKey')
    argParser.add_argument('-k', '--keep-sheet', help='Use this flag to stop the sheet from being cleared after merging', action='store_true')
    argParser.add_argument('-p', '--put-back', help='Put back failed merge entries into the spreadsheet.', action='store_true')
    argParser.add_argument('-d', '--dont-store', help='Will not store successful merges into history spreadsheet if this flag is present.', action='store_true')
    args = argParser.parse_args()

    if(args.keep_sheet is True and args.put_back is True):
        args.put_back = False

    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(googleAuthentification, scope)
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open_by_key(args.sheet)
    pSheet = spreadsheet.worksheet('Person')
    oSheet = spreadsheet.worksheet('Organization')
    pSheetH = None if args.dont_store is True else spreadsheet.worksheet('History_Person')
    oSheetH = None if args.dont_store is True else spreadsheet.worksheet('History_Organization')

    with connect('local') as dddb:
        print("===== [ Merge Started " + str(datetime.datetime.now()) + " utc ] =====")
        msgP = process_sheet(dddb, spreadsheet, pSheet, personType, args.keep_sheet, args.put_back, pSheetH)
        msgO = process_sheet(dddb, spreadsheet, oSheet, organizationType, args.keep_sheet, args.put_back, oSheetH)
        print("\n\n" + msgP + "\n" + msgO + "\n")
        print("===== [ Merge Ended " + str(datetime.datetime.now()) + " utc ] =====\n\n")
        logger.info(format_end_log("Output Summary", msgP, msgO))

if __name__ == '__main__':
    main()

