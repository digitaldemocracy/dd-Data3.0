'''
File: import_bills_ny.py
Author: John Alkire
Date: 11/26/2015
Description:
- Imports NY bills using senate API
- Fills Bill and BillVersion
- Currently configured to test DB
'''
import requests
import MySQLdb

def call_senate_api(restCall, year, house, offset):
    if house != "":
        house = "/" + house
    url = "http://legislation.nysenate.gov/api/3/" + restCall + "/" + str(year) + house + "?full=true&limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset=" + str(offset)
    r = requests.get(url)

    out = r.json()
    return (out["result"]["items"], out['total'])
    
def get_bills_api(year):
    total = 1000
    cur_offset = 0
    ret_bills = list()
    while cur_offset < total:
        call = call_senate_api("bills", 2015, "", cur_offset)
        bills = call[0]
        total = call[1]
        for bill in bills:             
            b = dict()
            b['number'] = bill['basePrintNo'][1:]
            b['type'] = bill['basePrintNo'][0:1]
            b['status'] = bill['status']['statusDesc']
            b['house'] = bill['billType']['chamber'].title()
            b['state'] = "NY"
            b['session'] = '0'
            b['sessionYear'] = year
            b['title'] = bill['title']
            b['versions'] = bill['amendments']['items']
            for key in b['versions'].keys():                
                b['versions'][key]['actClause'] = " "                
                
            b['bid'] = "NY_" + str(year) + str(year+1) + b['session'] + b['type'] + b['number']
            b['bid'] = b['bid']
            ret_bills.append(b)            
        cur_offset += 1000  
    print "Downloaded %d bills..." % len(ret_bills)    
    return ret_bills

def insert_bill_db(bill, dddb):
    insert_stmt = '''INSERT INTO Bill
                    (bid, number, type, status, house, state, session, sessionYear)
                    VALUES
                    (%(bid)s, %(number)s, %(type)s, %(status)s, %(house)s, %(state)s, %(session)s, %(sessionYear)s);
                    '''
    try:
        dddb.execute(insert_stmt, bill)
    except:
        #print "Bill exists"
        return False 
        
    return True

def insert_billversions_db(bill, dddb):    
    for key in bill['versions'].keys():
        bv = dict()
        bv['bid'] = bill['bid']
        bv['vid'] = bill['bid'] + key
        bv['date'] = bill['versions'][key]['publishDate']
        bv['state'] = "NY"
        bv['subject'] = bill['title']
        bv['title'] = bill['versions'][key]['memo']
        bv['text'] = bill['versions'][key]['fullText']
        insert_stmt = '''INSERT INTO BillVersion
                        (vid, bid, date, state, subject, title, text)
                        VALUES
                        (%(vid)s, %(bid)s, %(date)s, %(state)s, %(subject)s, %(title)s, %(text)s);
                        '''        
        try:
            dddb.execute(insert_stmt, bv)            
        except:
            continue
            #print "BV exists"
            #print (insert_stmt % bv)
        
        
def add_bills_db(year, dddb):
    bills = get_bills_api(year)
    bcount = 0

    for bill in bills:
        if insert_bill_db(bill, dddb):
            bcount = bcount + 1
        insert_billversions_db(bill, dddb)

    print "Inserted %d bills" % bcount
                    
def main():
    dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='JohnTest',
                        port=3306,
                        passwd='digitaldemocracy789')
    dddb = dddb_conn.cursor()
    dddb_conn.autocommit(True)
    #get_bills_api(2015)
    add_bills_db(2015, dddb)
main()