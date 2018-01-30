"""
File: FrontLog
Author: Nathan Philliber
Date: 1 November 2017

Description:
    - Manage front log
    - Generate sheets for people and organizations
    - Fill sheets with new organizations and suggested duplicates
    - Merge marked duplicates from same spreadsheet
"""

from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from apiclient import discovery
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from sys import version_info
from MergingUtilities.OrgMerge import *
import datetime
import argparse

people_spreadsheet_id = '<INSERT SPREADSHEET KEY HERE>'
organizations_spreadsheet_id = '1m1XlcRYJfwc3hsfBCdiiz5J_JrNpiRrawFeV2PCpNyc'
api_key_path = '/Users/Nathan/Documents/Programming_Projects/Digital_Democracy/dd-Data3.0/CurrentScripts/DDKEY'

sql_select_organizations_after_oid = '''SELECT oid, name, city, stateHeadquartered 
                                        FROM Organizations WHERE oid > %(oid)s;'''
sql_select_organizations_after_oid_with_limit = '''SELECT oid, name, city, stateHeadquartered FROM Organizations 
                                                   WHERE oid > %(oid)s ORDER BY oid LIMIT %(limit)s;'''
sql_select_organization_city = '''SELECT city FROM Organizations WHERE oid = %(oid)s;'''
sql_select_organization_state = '''SELECT stateHeadquartered FROM Organizations WHERE oid = %(oid)s;'''
sql_update_concept_name_and_canon_oid = '''UPDATE OrgConcept SET name = %(name)s, canon_oid = %(oid)s 
                                           WHERE oid = %(concept_oid)s;'''
sql_update_organization_name = '''UPDATE Organizations SET name = %(name)s WHERE oid = %(oid)s;'''
sql_update_organization_city = '''UPDATE Organizations SET city = %(city)s WHERE oid = %(oid)s;'''
sql_update_organization_state = '''UPDATE Organizations SET stateHeadquartered = %(state)s WHERE oid = %(oid)s;'''


def connect_to_sheets(auth_key_path):
    """
    Connect to Google Sheets API and return service object
    :param auth_key_path: path to json oauth2 key
    :return: Google api service
    """

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_key_path, scope)
    http = credentials.authorize(Http())
    discovery_url = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
    return discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discovery_url)


def get_sheet_id_from_title(service, spreadsheet_id, title):
    """
    Get the sheet id that matches the first instance of title
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id that sheet is contained in
    :param title: sheet name string
    :return: sheet id
    """

    spreadsheet_data = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet_data.get('sheets', '')
    for sheet in sheets:
        if sheet.get('properties', {}).get('title', '') == title:
            return sheet.get('properties').get('sheetId')
    return None


def duplicate_sheet(service, spreadsheet_id, template_sheet_id, new_title=None, index=0):
    """
    Create a copy of a sheet. Put the next sheet at the specified index
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id of sheets
    :param template_sheet_id: sheet to be copied
    :param new_title: title of copy (optional)
    :param index: index of where to put copy (default is first/0)
    :return: return title of sheet copy
    """

    body = {
        "requests": [{
            "duplicateSheet": {
                "newSheetName": new_title,
                "insertSheetIndex": index,
                "sourceSheetId": template_sheet_id
            }
        }]
    }

    result = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    return result.get('replies', {})[0].get('duplicateSheet', {}).get('properties', {}).get('title', '')


def protect_columns(service, spreadsheet_id, sheet_title, first=1, last=26, description="Don't Edit"):
    """
    Protect all columns in specified range
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id
    :param sheet_title: title of sheet
    :param first: first column to protect
    :param last: last column to protect
    :param description: description of protected range
    """

    sheet_id = get_sheet_id_from_title(service, spreadsheet_id, sheet_title)

    body = {
        "requests": [{
            "addProtectedRange": {
                "protectedRange": {
                    "range": {
                        "endColumnIndex": last,
                        "sheetId": sheet_id,
                        "startColumnIndex": first
                    },
                    "description": description,
                    "warningOnly": 'True'
                }
            }
        }]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def find_previous_sheet_by_date(service, spreadsheet_id, max_sheet_title=None):
    """
    Find the id of the sheet that became before this one (by date in sheet's title)
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id that contains sheet
    :param max_sheet_title: title of sheet, should be "DATE Organization/or/Person"
    :return: sheet title of found sheet
    """

    if max_sheet_title is not None:
        cur_date = max_sheet_title.split(' ')[0]
    else:
        cur_date = None

    spreadsheet_data = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet_data.get('sheets', '')

    cur_highest = '.'  # Lowest ASCII value

    for sheet in sheets:
        title = sheet.get('properties', {}).get('title', '')
        date = title.split(' ')[0]
        if date[0].isdigit() and cur_highest.split(' ')[0] < date:
            if cur_date is None or date < cur_date:
                cur_highest = title

    if cur_highest == '.':
        cur_highest = None

    return cur_highest


def find_highest_id_in_column(service, spreadsheet_id, sheet_title, column='C', start_row=2):
    """
    Find the largest id in a column, useful to know where to continue loading entities from
    :param service: Google API service
    :param spreadsheet_id: Spreadsheet id
    :param sheet_title: Title of sheet to search
    :param column: column name to search in
    :param start_row: row to start search at (i.e. skip the title row/s)
    :return: id
    """

    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range="'"+sheet_title+"'!" +
                                                 column + str(start_row) + ":" + column).execute()

    cur_max = result['values'][0][0]
    for value in result['values']:
        if int(value[0]) > int(cur_max):
            cur_max = value[0]

    return cur_max


def get_organization_data(last_processed_oid, limit=None):
    """
    :param last_processed_oid: the last oid that was processed by this script,
                                display all organizations that have higher oids
    :param limit: limit the number of organizations that should be put into the sheet
    :return: list containing all new organizations in following format:
            [{oid, name, city, state, suggested:[{oid, name, city, state}, ...]}, ...]
    """

    with connect('local', logger=create_logger()) as dddb:
        dddb.execute(sql_select_organizations_after_oid if limit is None
                     else sql_select_organizations_after_oid_with_limit, {'oid': last_processed_oid, 'limit': limit})
        results = dddb.fetchall()

        data = []
        for result in results:
            data.append({"oid": result[0], "name": result[1], "city": result[2], "state": result[3],
                         "suggested": []})

        return data


def populate_organizations_sheet(service, spreadsheet_id, sheet_title, start_search_oid=None, limit=None):
    """
    Fill the sheet with new organizations and suggestions for merges.
    Assumes sheet is copy of template (but otherwise empty)
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id
    :param sheet_title: title of sheet to fill
    :param start_search_oid: the oid of the organization to start load from. If none, will start from last sheet
    :param limit: limit the number of organizations that should be put into the sheet
    """

    if start_search_oid is None:
        last_sheet = find_previous_sheet_by_date(service, spreadsheet_id, sheet_title)
        last_highest_oid = find_highest_id_in_column(service, spreadsheet_id, last_sheet)
    else:
        last_highest_oid = start_search_oid

    data = get_organization_data(last_highest_oid, limit)

    values = []
    for org_data in data:
        row = ['[Suspected]' if len(org_data['suggested']) > 0 else '', org_data['oid'], org_data['name'],
               org_data['city'], org_data['state']]

        for suggest in org_data['suggested']:
            suggest_cel = 'OID: ' + str(suggest['oid']) + ', Name: ' + str(suggest['name']) + ', City: ' + \
                            str(suggest['city']) + ', State: ' + str(suggest['state'])

            row.append(suggest_cel)
        values.append(row)

    body = {"values": values}

    sheet_range = "'"+sheet_title+"'!B2"

    service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=sheet_range,
                                           valueInputOption='USER_ENTERED', body=body).execute()


def update_column(service, spreadsheet_id, sheet_name, value_list, column='A'):
    """
    Update values in a column.
    :param service: Google api service
    :param spreadsheet_id: id of spreadsheet
    :param sheet_name: sheet title
    :param value_list: list of values to update in column. None = no update
    :param column: which column to update
    """

    body = {
        "values": [value_list],
        "majorDimension": 'COLUMNS'
    }

    service.spreadsheets().values().update(spreadsheetId=spreadsheet_id,
                                           body=body,
                                           range="'"+sheet_name+"'!"+str(column)+"2:"+str(column),
                                           valueInputOption='USER_ENTERED').execute()


def paint_column_colors(service, spreadsheet_id, sheet_id, value_list, column=0, no_color_none=False):
    """
    Color a column based on values in list
    :param service: Google api service
    :param spreadsheet_id: id of spreadsheet
    :param sheet_id: id of sheet (not sheet title)
    :param value_list: list of values, either True (green), False (red), or None (gray)
    :param column: the column to set colors in
    :param no_color_none: True if don't want to color on None, false to color None values gray
    """

    if True not in value_list and False not in value_list:
        return

    requests = []

    red = {"userEnteredFormat": {"backgroundColor": {"red": 1.0, "green": 0.2, "blue": 0.2, "alpha": 1.0}}}
    green = {"userEnteredFormat": {"backgroundColor": {"red": 0.2, "green": 1.0, "blue": 0.2, "alpha": 1.0}}}
    gray = {"userEnteredFormat": {"backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85, "alpha": 1.0}}}

    last_row = 0

    for value in value_list:
        last_row += 1
        values = []
        if value is None and no_color_none is False:
            values.append(gray)
        elif value is False:
            values.append(red)
        elif value is True:
            values.append(green)

        if values:
            requests.append({
                "updateCells": {
                    "rows": [{
                        "values": values
                    }],
                    "fields": 'userEnteredFormat.backgroundColor',
                    "range": {
                        "sheetId": sheet_id,
                        "startColumnIndex": column,
                        "endColumnIndex": column + 1,
                        "startRowIndex": last_row,
                        "endRowIndex": last_row + 1
                    }
                }
            })

    body = {"requests": requests}

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def get_organization_suggestions(dddb, oid, name, city, state):
    """
    Return a list of possible organization matches
    :param dddb: Database connection
    :param oid: oid of organization
    :param name: name of organization
    :param city: city of organization
    :param state: state of organization
    :return: a list of possible organization matches, or empty if none
            format: [{oid, name, city, state}, ...]
    """

    # Make sure that the suggestion matches include matches from the new stuff, this solves concept canon oid problem


def try_update_org_name(dddb, oid, new_name):
    """
    Update the organization name if provided name is different
    :param dddb: Database connection
    :param oid: oid of organization
    :param new_name: name that organization should be named
    :return: {old_name, new_name} or None if no change
    """

    dddb.execute(sel_org_name, {'oid': oid})
    if dddb.rowcount == 0:
        return None
    old_name = dddb.fetchone()[0]

    if old_name != new_name and "(previously \"" not in new_name:
        dddb.execute(sql_update_organization_name, {'oid': oid, 'name': new_name})
        print("Changed name of '" + old_name + "' to '" + new_name + "'")
        return {'old_name': old_name, 'new_name': new_name}
    return None


def try_update_city(dddb, oid, new_city):
    """
    Update the organization's city if the provided city is different
    :param dddb: Database connection
    :param oid: oid of organization to update
    :param new_city: city string
    :return: {old_city, new_city} or None if no change
    """

    dddb.execute(sql_select_organization_city, {'oid': oid})
    if dddb.rowcount == 0 or new_city == "":
        return None
    old_city = dddb.fetchone()[0]

    if old_city != new_city and "previously \"" not in new_city:
        print("Changed city of organization " + str(oid) + " from '" + str(old_city) + "' to '" + new_city + "'")
        dddb.execute(sql_update_organization_city, {'oid': oid, 'city': new_city})

        return {'old_city': old_city, 'new_city': new_city}
    return None


def try_update_state(dddb, oid, new_state):
    """
    Update the organization's state if the provided state is different
    :param dddb: Database connection
    :param oid: oid of organization to update
    :param new_state: state string
    :return: {old_state, new_state} or None if no change
    """

    dddb.execute(sql_select_organization_state, {'oid': oid})
    if dddb.rowcount == 0 or new_state == "":
        return None
    old_state = dddb.fetchone()[0]

    if old_state != new_state and "previously \"" not in new_state:
        print("Changed state headquarters of organization " + str(oid) + " from '" + str(old_state) +
              "' to '" + new_state + "'")
        dddb.execute(sql_update_organization_state, {'oid': oid, 'state': new_state})

        return {'old_state': old_state, 'new_state': new_state}
    return None


def process_organization_sheet_merges(service, spreadsheet_id, sheet_title):
    """
    Merge organizations indicated on a google sheet
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id
    :param sheet_title: sheet title to get merge pairs from
    :return: {num_success, num_failed}
    """

    result = service.spreadsheets().values().batchGet(spreadsheetId=spreadsheet_id, ranges=[
        "'" + sheet_title + "'!A2:A",
        "'" + sheet_title + "'!C2:C",
        "'" + sheet_title + "'!D2:D",
        "'" + sheet_title + "'!E2:E",
        "'" + sheet_title + "'!F2:F"]).execute()

    wrapper_connection = connect('local', logger=create_logger())
    with wrapper_connection as dddb:

        # Check for name changes
        new_names = []
        paint_results = []
        for oid, cur_name in zip(result['valueRanges'][1]['values'], result['valueRanges'][2]['values']):
            update_result = try_update_org_name(dddb, oid[0], cur_name[0])
            paint_results.append(None if update_result is None else True)
            new_names.append(None if update_result is None else (update_result['new_name'] + " (previously \"" +
                                                                 update_result['old_name'] + "\")"))

        paint_column_colors(service, spreadsheet_id, get_sheet_id_from_title(service, spreadsheet_id, sheet_title),
                            paint_results, column=3, no_color_none=True)
        update_column(service, spreadsheet_id, sheet_title, new_names, column='D')

        # Check for city changes
        new_cities = []
        paint_results = []
        for oid, cur_city in zip(result['valueRanges'][1]['values'], result['valueRanges'][3]['values']):
            update_result = try_update_city(dddb, oid[0], "" if len(cur_city) == 0 else cur_city[0])
            paint_results.append(None if update_result is None else True)
            new_cities.append(None if update_result is None else (str(update_result['new_city']) + " (previously \"" +
                                                                  str(update_result['old_city']) + "\")"))

        paint_column_colors(service, spreadsheet_id, get_sheet_id_from_title(service, spreadsheet_id, sheet_title),
                            paint_results, column=4, no_color_none=True)
        update_column(service, spreadsheet_id, sheet_title, new_cities, column='E')

        # Check for state changes
        new_states = []
        paint_results = []
        for oid, cur_state in zip(result['valueRanges'][1]['values'], result['valueRanges'][4]['values']):
            update_result = try_update_state(dddb, oid[0], "" if len(cur_state) == 0 else cur_state[0])
            paint_results.append(None if update_result is None else True)
            new_states.append(None if update_result is None else (str(update_result['new_state']) + " (previously \"" +
                                                                  str(update_result['old_state']) + "\")"))

        paint_column_colors(service, spreadsheet_id, get_sheet_id_from_title(service, spreadsheet_id, sheet_title),
                            paint_results, column=5, no_color_none=True)
        update_column(service, spreadsheet_id, sheet_title, new_states, column='F')

        # Make sure that there are some merge requests
        if 'values' not in result['valueRanges'][0] or 'values' not in result['valueRanges'][1]:
            print("Did not find anything to merge in " + sheet_title)
            return {'num_success': 0, 'num_failed': 0}

        # Check for merge requests
        paint_results = []
        for good_oid, bad_oid in zip(result['valueRanges'][0]['values'], result['valueRanges'][1]['values']):
            if good_oid != [] and bad_oid != []:
                paint_results.append(merge_organization_pair(wrapper_connection, dddb, good_oid[0], bad_oid[0]))
            else:
                paint_results.append(None)

        paint_column_colors(service, spreadsheet_id, get_sheet_id_from_title(service, spreadsheet_id, sheet_title),
                            paint_results)

        protect_columns(service, spreadsheet_id, sheet_title, first=0, last=26,
                        description="This sheet has already been merged. Do not edit.")


def merge_organization_pair(wrapper_connection, dddb, good_oid, bad_oid):
    """
    Merge bad_oid into good_oid
    :param wrapper_connection: MySQL_Wrapper object
    :param dddb: Database connection
    :param good_oid: The organization to be merged into
    :param bad_oid: The organization to be merged
    :return: true/false for success/failure
    """

    #   |---------------------------------|
    #   |   |                 good        |
    #   |---|-----------------------------|
    #   | b |           concept   indep   |
    #   | a | concept  |   A        B     |
    #   | d | indep    |   C        D     |
    #   |---------------------------------|

    try:
        good_concept_oid = has_org_concept(dddb, {'oid': good_oid}, True)
        bad_concept_oid = has_org_concept(dddb, {'oid': bad_oid}, True)

        if good_concept_oid != 0:

            # A: good:concept - bad:concept
            if bad_concept_oid != 0:

                print("[Case: Good=Concept, Bad=Concept]")
                print("Currently no support for merging two concept organizations.")

                if good_concept_oid == bad_concept_oid:
                    print("These organizations are already in the same concept.")

                wrapper_connection.connection.commit()
                return False

            # C: good:concept - bad:indep
            #    Merge bad into good concept
            else:
                print("[Case: Good=Concept, Bad=Independent] " + str(bad_oid) + " -> " + str(good_concept_oid))
                merge_org(dddb, {'good_oid': good_oid, 'bad_oid': bad_oid, 'is_subchapter': False}, throw_exc=True)
                merge_org_concept(dddb, {'good_oid': good_concept_oid, 'bad_oid': bad_oid, 'is_subchapter': False},
                                  is_org_concept=True, throw_exc=True)
                wrapper_connection.connection.commit()
                return True
        else:

            # B: good:indep - bad:concept
            #    Merge good into bad then change canon_oid and name to good
            if bad_concept_oid != 0:
                print("[Case: Good=Independent, Bad=Concept] " + str(good_oid) + " -> " + str(bad_concept_oid) +
                      ", then change concept name/canon_oid")
                merge_org(dddb, {'good_oid': bad_oid, 'bad_oid': good_oid, 'is_subchapter': False}, throw_exc=True)
                merge_org_concept(dddb, {'good_oid': bad_concept_oid, 'bad_oid': good_oid, 'is_subchapter': False},
                                  throw_exc=True)
                dddb.execute(sql_update_concept_name_and_canon_oid, {'oid': good_oid, 'canon_oid': bad_concept_oid,
                                                                     'name': get_org_name(dddb, {'oid': good_oid})})
                wrapper_connection.connection.commit()
                return True

            # D: good:indep - bad:indep
            #    Create new org concept
            else:
                print("[Case: Good=Independent, Bad=Independent] " + str(bad_oid) + " -> " + str(good_oid) +
                      " -> new concept organization")
                good_concept_oid = add_org_concept(dddb, {'good_oid': good_oid,
                                                          'concept': get_org_name(dddb, {'oid': good_oid})})
                merge_org(dddb, {'good_oid': good_oid, 'bad_oid': bad_oid, 'is_subchapter': False}, throw_exc=True)
                merge_org_concept(dddb, {'good_oid': good_concept_oid, 'bad_oid': bad_oid, 'is_subchapter': False},
                                  is_org_concept=True, throw_exc=True)
                wrapper_connection.connection.commit()
                return True
    except:
        wrapper_connection.connection.rollback()
        print("FAILED to merge. Rolling back...")
        return False


def get_people_data(time_period):
    """
    :param time_period: how long ago to select people from (seconds)
    :return: list containing all new organizations in following format:
            [{pid, first, last, middle, image, suggested:[{pid, first, last, middle, image}, ...]}, ...]
    """


def populate_people_sheet(service, spreadsheet_id, sheet_id):
    """
    Fill the sheet with new people and suggestions for merges.
    Assumes sheet is copy of template (but otherwise empty)
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id
    :param sheet_id: sheet id to fill
    """


def process_people_sheet_merges(service, spreadsheet_id, sheet_id):
    """
    Merge people indicated on a google sheet
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id
    :param sheet_id: sheet id to get merge pairs from
    :return:
    """


def generate_organizations_sheet(service, spreadsheet_id, start_oid=None, date=None, limit=None):
    """
    Generate a new sheet, populated with recent organizations
    :param service: Google api service
    :param spreadsheet_id: spreadsheet id of spreadsheet to put sheet in
    :param start_oid: the organization to start from
    :param date: the date of the sheet to check for last organization
    :param limit: the limit for number of organizations to put into sheet
    """

    # Get correct date for sheet
    if date is not None:
        # Lazy format checking
        date = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
    else:
        date = datetime.datetime.now().strftime("%Y-%m-%d")

    # Make sure that there's a good last sheet or --start is there
    last_sheet = find_previous_sheet_by_date(service, spreadsheet_id, date + " Organizations")
    if last_sheet is None and start_oid is None:
        print("No previous sheet to find starting oid from. Use --start <OID> to specify a starting point.")
        exit(-1)

    # Create new sheet from template
    template_sheet_id = get_sheet_id_from_title(service, spreadsheet_id, "TEMPLATE")
    new_sheet_title = duplicate_sheet(service, spreadsheet_id, template_sheet_id, date + " Organizations")

    # Populate the sheet with organization data
    populate_organizations_sheet(service, spreadsheet_id, new_sheet_title, start_oid, limit)
    protect_columns(service, spreadsheet_id, new_sheet_title, first=1, last=2)
    protect_columns(service, spreadsheet_id, new_sheet_title, first=6, last=26)


def main():

    # Arg parser setup

    arg_parser = argparse.ArgumentParser(description='Front Log Utility. Load recently added organizations/people' +
                                         ' into a google spreadsheet. Merge indicated pairs on Google sheet back' +
                                         ' into the database.')

    arg_parser.add_argument('-p', '--people', help='Use the people spreadsheet.', action='store_true')
    arg_parser.add_argument('-o', '--organizations', help='Use the organizations spreadsheet.', action='store_true')

    action_group = arg_parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('-g', '--generate', help='Generate a spreadsheet from database', action='store_true')
    action_group.add_argument('-m', '--merge', help='Merge indicated from sheet to database.', action='store_true')

    arg_parser.add_argument('-d', '--date', help='The date to call this sheet. Default is today. Format: '
                                                 'year-month-day. Picking a date that takes place before other '
                                                 'already existing sheets will result in weird behavior. If you do '
                                                 'need to do this, it is recommended that you also use -start and '
                                                 'specify a start id point.',
                            type=str, default=None)
    arg_parser.add_argument('-s', '--start', help='The id to start filling sheet from. (Not recommended option.) '
                                                  'By default, sheet will fill based on last generated sheet',
                            type=str, default=None)
    arg_parser.add_argument('-l', '--limit', help='Limit the number of entities to fill when generating sheet.',
                            type=int, default=None)
    arg_parser.add_argument('-f', '--force', help='Bypass confirmation dialog.', action='store_true')

    args = arg_parser.parse_args()

    if args.people is False and args.organizations is False:
        arg_parser.error('Process people and/or organizations? Must specify -p and/or -o')

    if args.people and args.organizations and args.start:
        arg_parser.error('Start id parameter not compatible with both people and organizations at once.' +
                         ' Choose -p or -o to use -start.')

    # Build confirmation message

    conf_msg = '\nYou are about to '
    if args.generate:
        conf_msg += 'GENERATE sheet' + ('s for both people and organizations.' if args.people and args.organizations
                                        else ' for ' + ('people.' if args.people else 'organizations.'))
        if args.date is not None:
            conf_msg += '\nThe sheet' + ('s' if args.people and args.organizations else '') + \
                        ' will be marked with ' + args.date + ' as the date.'

        if args.start is not None:
            conf_msg += '\nThe sheet will be filled with ids starting from ' + args.start + '.'

        if args.limit is not None:
            conf_msg += '\nThe number of entities in the sheet will be limited to ' + str(args.limit) + '.'

    if args.merge:
        conf_msg += 'MERGE sheet' + ('s for both people and organizations.' if args.people and args.organizations
                                     else ' for ' + ('people.' if args.people else 'organizations.'))
        conf_msg += '\nWill use ' + ('most recent sheet.' if args.date is None else 'sheet from ' + args.date + '.')

    conf_msg += '\n\nWould you like to continue? (y/n): '

    py3 = version_info[0] > 2

    # Display confirmation if necessary and run operations

    if args.force or (py3 and input(conf_msg).lower() == 'y') or (py3 is False and raw_input(conf_msg).lower() == 'y'):
        service = connect_to_sheets(api_key_path)

        if args.organizations and args.generate:
            generate_organizations_sheet(service, organizations_spreadsheet_id, args.start, args.date, args.limit)

        if args.organizations and args.merge:
            process_organization_sheet_merges(service, organizations_spreadsheet_id,
                                              find_previous_sheet_by_date(service, organizations_spreadsheet_id)
                                              if args.date is None else args.date + " Organizations")


if __name__ == '__main__':
    main()
