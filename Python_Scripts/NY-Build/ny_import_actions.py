#!/usr/bin/python
# -*- coding: utf8 -*-

'''
    File: ny_import_actions.py
    Author: Miguel Aguilar
    Date: 06/23/2016

    Description:
        - Fills in the Action table with NY data
'''

from Database_Connection import mysql_connection
import requests
import MySQLdb
import traceback
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None
INSERTED = 0

insert_action = '''INSERT INTO Action 
                    (bid, date, text)
                    VALUES
                    (%(bid)s, %(date)s, %(text)s);'''

select_action = '''SELECT bid, text 
                 FROM Action   
                 WHERE bid = %(bid)s;'''


API_YEAR = 2016
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&" 
API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}&"
STATE = 'NY'                 
BILL_API_INCREMENT = 1000

def create_payload(table, sqlstmt):                                             
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'NY'
    }

#calls NY Senate API and returns a tuple with the list of results and the total number of results                        
def call_senate_api(restCall, house, offset, resolution):
    if house != "":
        house = "/" + house
    
    if resolution:
        api_url = API_URL+"term=billType.resolution:true"
    else:
        api_url = API_URL+"term=billType.resolution:false"

    url = api_url.format(restCall, API_YEAR, house, offset)
    r = requests.get(url)
    out = r.json()
    
    return (out["result"]["items"], out['total'])

#function to compile all bill data from NY senate API. There are over 1000 bills
#so the API is looped over in 1000 bill increments. Data is placed into lists
#and dictionaries. List of bill dictionaries is returned    
def get_bills_api(resolution):
    total = BILL_API_INCREMENT
    cur_offset = 0
    ret_bills = list()
    
    while cur_offset < total:
        call = call_senate_api("bills", "/search", cur_offset, resolution)
        bills = call[0]
        total = call[1]
        
        for bill in bills:
            bill = bill['result']             
            b = dict()
            b['number'] = bill['basePrintNo'][1:]
            b['type'] = bill['basePrintNo'][0:1]
            b['session'] = '0'
            b['actions'] = bill['actions']['items']   
            b['bid'] = STATE + "_" + str(bill['session']) + str(int(bill['session'])+1) 
            b['bid'] += b['session'] + b['type'] + b['number']            
            ret_bills.append(b)  
                      
        cur_offset += BILL_API_INCREMENT
        
    #print "Downloaded %d bills..." % len(ret_bills)    
    return ret_bills

def is_act_in_db(act, dddb):
    dddb.execute(select_action, act)
    act_list = dddb.fetchall()

    if (act['bid'], act['text']) in act_list:
        return True
    return False

def insert_actions_db(bill, dddb):
    global INSERTED
    for action in bill['actions']:
        act = dict()
        act['bid'] = bill['bid']
        act['date'] = action['date']
        act['text'] = action['text']

        if not is_act_in_db(act, dddb):
            try:
                dddb.execute(insert_action, act)
                INSERTED += dddb.rowcount
            except MySQLdb.Error:
                logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload('Action',(insert_action%act)))
            return True
        return False

#function to loop over all bills and insert bill actions        
def add_bill_actions_db(dddb):
    #Resolution passed in, if 'True' then gets all the resolutions
    bills = get_bills_api(False)
    bills.extend(get_bills_api(True))
    act_count = 0

    for bill in bills:
        if insert_actions_db(bill, dddb):
            act_count = act_count + 1

    print "Inserted %d actions" % act_count

def main():
#    with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
#                        user='awsDB',
#                        db='DDDB2015Dec',
#                        port=3306,
#                        passwd='digitaldemocracy789',
#                        charset='utf8') as dddb:
        dddb = mysql_connection() 
        add_bill_actions_db(dddb)      
        logger.info(__file__ + ' terminated successfully.', 
            full_msg='inserted ' + str(INSERTED) + ' rows in Action',
            additional_fields={'_affected_rows':'Action:'+str(INSERTED),
                               '_inserted':'Action:'+str(INSERTED),
                               '_state':'NY'})
    
if __name__ == '__main__':
    with GrayLogger(GRAY_URL) as _logger:
        logger = _logger
        main()
