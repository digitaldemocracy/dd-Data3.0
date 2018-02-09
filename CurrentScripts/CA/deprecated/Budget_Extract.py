#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: Budget_Extract.py

Description: Gets budget pdfs from ebudget.ca.gov. Inserts them as bills with 
             type BUD. URLs are provided on a spreadsheet called
             "budgetline items.xlsx"

Tables Affected: 
    - Bill
    - BillVersion
'''

#from Database_Connection import mysql_connection
import os
import requests
import MySQLdb
import sys
import traceback
import re
import time
import pandas as pd
import subprocess
from datetime import datetime
from bs4 import BeautifulSoup
#from graylogger.graylogger import GrayLogger
#GRAY_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
#logger = None

STATE = "CA"

#global counters
I_B = 0          #Bill inserts
I_BV = 0         #BillVesrion inserts

S_BILL = '''SELECT bid
            FROM Bill
            WHERE bid = %s'''
S_BILLVERSION = '''SELECT vid
                   FROM BillVersion
                   WHERE vid = %s'''

I_BILL = '''INSERT INTO Bill (bid, type, number, billState, status, house, session, state, sessionYear, visibility_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
I_BILLVERSION = '''INSERT INTO BillVersion (vid, bid, billState, subject, title, state)
                   VALUES (%s, %s, %s, %s, %s, %s)'''

def create_payload(table, sqlstmt):
    return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'CA'
    }

'''
takes in a xlsx file with appropriate info
returns a list of budgets to download and insert
each item in the list is a dictionary
'''
def read_spreadsheet(file):
    pdFile = pd.ExcelFile(file)
    sheet = pdFile.parse(0)

    result = []
    for row in sheet.iterrows():
        if str(row[1]['CATEGORY CODE']) != "CATEGORY CODE":
            d = dict()
            d["cCode"] = str(row[1]['CATEGORY CODE'])
            d["dCode"] = str(row[1]['DEPARTMENT CODE'])
            d["dept"] = str(row[1]['DEPARTMENT'])
            d["url"] = str(row[1]['LINK TO BUDGET DETAIL'])
            result.append(d)

    return result
      
'''
if bid is in db, it will return a bid
else returns None
'''
def get_bid(cursor, bid):
    result = None
    cursor.execute(S_BILL, (bid, ))
    if cursor.rowcount > 0:
        result = cursor.fetchone()[0]

    return result

'''
if billversion is in db, it will return a vid
else returns None
'''
def get_vid(cursor, vid):
    result = None
    cursor.execute(S_BILLVERSION, (vid, ))
    if cursor.rowcount > 0:
        result = cursor.fetchone()[0]

    return result

'''
checks to see if bill is already in db,
if not it will try to insert
'''
def insert_bill(cursor, bid, number, sessionYear):
    global I_B
    
    bType = "BUD"
    billState = ""
    status = ""
    house = "Governor"
    session = 0
    visibility_flag = 1

    if get_bid(cursor, bid) == None:
        try:
            cursor.execute(I_BILL, (bid, bType, number, billState, status, house, session, STATE, sessionYear, visibility_flag))
            I_B += cursor.rowcount
        except MySQLdb.Error:
            print("died")
            # logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            #     additional_fields=create_payload('Bill',(I_BILL % (bid, bType, number, billState, status, house, session, STATE, sessionYear, visibility_flag))))


'''
check to see if billversion is in db,
if not it will try to insert
'''
def insert_billversion(cursor, bid, subject):
    global I_BV

    billState = "Chaptered"

    if get_vid(cursor, bid) == None:
        try:
            cursor.execute(I_BILLVERSION, (bid, bid, billState, subject, subject, STATE))
            I_BV += cursor.rowcount
        except MySQLdb.Error:
            print("here")
            # logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            #     additional_fields=create_payload('BillVersion',(I_BILLVERSION % (bid, bid, billState, subject, subject, STATE))))
'''
downloads the budget pdfs into specified directory
|pdfDir|: directory to download the pdfs in
'''
def download_pdf(pdfDir, url, bid):
    filename = bid + ".pdf"
    result = subprocess.call(
            "wget -q -t 5 -O {0} {1}".format(pdfDir + "/" + filename, url),
            shell=True)
    if result:
        print "Attempted to download {0}, got error-code {1}".format(url, result)
    else:
        print "Downloaded {0} successfully, named {1}".format(url, bid)

    return result

def main():
    #ddinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host='dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                                   port=3306,
                                   db='DDDB2016Aug',
                                   user='dbMaster',
                                   passwd=os.environ["DBMASTERPASSWORD"],
                                   charset='utf8') as dddb:
        
        year = datetime.now().year
        yearStr = str(year) + "-" + str(year + 1)[2:]

        if year % 2 == 0:
            sessionYear = year - 1
        else:
            sessionYear = year

        pdfDir = str(year) + "budgetPDFs"
        #creates the pdf directory
        subprocess.call("mkdir " + pdfDir, shell=True)
        pdfCount = 0        

        budgetList = read_spreadsheet("budgetlineitems.xlsx")
        for budget in budgetList:
            print(budget)
            #assuming we are using a spreadsheet that has 2018-19 in urls
            if "2018-19" in budget["url"]:
                url = budget["url"]
                tempUrl = url.split("2018-19")
                url = tempUrl[0] + yearStr  + tempUrl[1]

                bid = "CA_" + str(year) + str(year + 1) + "BUD" + budget["dCode"]
                #if downloading pdf doesnt return an error
                #then we will try to insert
                if not download_pdf(pdfDir, url, bid):
                    pdfCount += 1
                    insert_bill(dddb, bid, budget["dCode"], sessionYear)
                    insert_billversion(dddb, bid, budget["dept"])

       
        print("here")
        # logger.info(__file__ + ' terminated successfully.',
        #     full_msg='Inserted ' + str(I_B) + ' rows in Bill and inserted '
        #               + str(I_BV) + ' rows in BillVersion',
        #     additional_fields={'_affected_rows':'Bill:'+ str(I_B) +
        #                                    ', BillVersion:'+ str(I_BV),
        #                        '_inserted':'Bill:'+ str(I_B) +
        #                                    ', BillVersion:' + str(I_BV),
        #                        '_state':'CA'})
        
        print "Downloaded", str(pdfCount), "pdfs"
        print "Inserted", str(I_B), "rows into Bill"
        print "Inserted", str(I_BV), "rows into BillVersion"

if __name__ == '__main__':
    #with GrayLogger(GRAY_URL) as _logger:
    #logger = _logger
    main()
