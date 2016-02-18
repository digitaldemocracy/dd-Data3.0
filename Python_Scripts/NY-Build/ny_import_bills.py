#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
File: ny_import_bills.py
Author: John Alkire
Date: 11/26/2015
Description:
- Imports NY bills using senate API
- Fills Bill and BillVersion
- Currently configured to test DB
'''
import requests
import MySQLdb
import loggingdb

update_billversion = '''UPDATE BillVersion
                        SET bid = %(bid)s, date = %(date)s, state = %(state)s, 
                         subject = %(subject)s, title = %(title)s, text = %(text)s                    
                        WHERE vid = %(vid)s;'''

update_bill =  '''UPDATE Bill
                  SET number = %(number)s, type = %(type)s, status = %(status)s, 
                   house = %(house)s, state = %(state)s, session = %(session)s, 
                   sessionYear = %(sessionYear)s                    
                  WHERE bid = %(bid)s;'''
                
insert_bill = '''INSERT INTO Bill
                  (bid, number, type, status, house, state, session, sessionYear)
                 VALUES
                  (%(bid)s, %(number)s, %(type)s, %(status)s, %(house)s, %(state)s,
                  %(session)s, %(sessionYear)s);'''                
                
insert_billversion = '''INSERT INTO BillVersion
                         (vid, bid, date, state, subject, title, text)
                        VALUES
                         (%(vid)s, %(bid)s, %(date)s, %(state)s, %(subject)s, %(title)s,
                         %(text)s);''' 
                                               
insert_billversion = '''INSERT INTO BillVersion
                         (vid, bid, date, state, subject, title, text)
                        VALUES
                         (%(vid)s, %(bid)s, %(date)s, %(state)s, %(subject)s, %(title)s,
                         %(text)s);'''                            
API_YEAR = 2016
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&" 
API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"
STATE = 'NY'                 
BILL_API_INCREMENT = 1000

#calls NY Senate API and returns a tuple with the list of results and the total number of results                        
def call_senate_api(restCall, house, offset):
    if house != "":
        house = "/" + house
    
    url = API_URL.format(restCall, API_YEAR, house, offset)
    r = requests.get(url)
    out = r.json()
    
    return (out["result"]["items"], out['total'])

#function to compile all bill data from NY senate API. There are over 1000 bills
#so the API is looped over in 1000 bill increments. Data is placed into lists
#and dictionaries. List of bill dictionaries is returned    
def get_bills_api():
    total = BILL_API_INCREMENT
    cur_offset = 0
    ret_bills = list()
    while cur_offset < total:
        call = call_senate_api("bills", "", cur_offset)
        bills = call[0]
        total = call[1]
        for bill in bills:             
            b = dict()
            b['number'] = bill['basePrintNo'][1:]
            b['type'] = bill['basePrintNo'][0:1]
            b['status'] = bill['status']['statusDesc']
            b['house'] = bill['billType']['chamber'].title()
            b['state'] = STATE
            b['session'] = '0'
            b['sessionYear'] = bill['session']
            b['title'] = bill['title']
            b['versions'] = bill['amendments']['items']    
            b['bid'] = STATE + "_" + str(bill['session']) + str(int(bill['session'])+1) 
            b['bid'] += b['session'] + b['type'] + b['number']            
            ret_bills.append(b)            
        cur_offset += BILL_API_INCREMENT
        
    #print "Downloaded %d bills..." % len(ret_bills)    
    return ret_bills                     
                
#function to insert a new bill or update an existing one. Returns true if
#insertion occurs, false otherwise                          
def insert_bill_db(bill, dddb):
    try:                        
        dddb.execute(insert_bill, bill)        
    except:        
        dddb.execute(update_bill, bill)  
        return False         
    return True

#function to parse over and insert billversions. Called once per bill;
#inserts or updates corresponding number of bill versions
def insert_billversions_db(bill, dddb):    
            
    for key in bill['versions'].keys():
        bv = dict()
        bv['bid'] = bill['bid']        
        bv['vid'] = bill['bid'] + key
        bv['date'] = bill['versions'][key]['publishDate']
        bv['state'] = STATE
        bv['subject'] = bill['title']
        bv['title'] = bill['versions'][key]['actClause']
        bv['text'] = bill['versions'][key]['fullText']
        
        try:
            dddb.execute(insert_billversion, bv)            
        except:            
            dddb.execute(update_billversion, bv)
        
#function to loop over all bills and insert bills and bill versions        
def add_bills_db( dddb):
    bills = get_bills_api()
    bcount = 0

    for bill in bills:
        if insert_bill_db(bill, dddb):
            bcount = bcount + 1
        insert_billversions_db(bill, dddb)

    print "Inserted %d bills" % bcount
                    
def main():
    dddb_conn =  loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='DDDB2015Dec',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8')
    dddb = dddb_conn.cursor()
    dddb_conn.autocommit(True)
    add_bills_db(dddb)
    
main()
