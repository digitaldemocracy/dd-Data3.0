#!/usr/bin/env python27
# -*- coding: utf-8 -*- 
'''
File: Budget_Extract.py
Author: Matt Versaggi
Modified By: N/A
Date: 2/8/2016
Last Modified: N/A

Description:
- Scrapes the CA budget website for the list of budget line items for a given
    year, inserting them into the DB
- Also downloads the pdf linked to a given line item (if available) and places
    it in a directory called "pdfs" located in the same working directory that
    this script is run from
- Used for annual update

Sources:
  - Governor's Budget website (http://www.ebudget.ca.gov/2016-17/deptIndexByCode.html)

Populates:
  - Bill (bid, type, number, billState, status, house,
     session, sessionYear, state)
  - BillVersion (vid, bid, date, billState, subject, appropriation,
     substantive_changes, title, digest, text, state)
'''

import MySQLdb
import urllib2
import subprocess
import re
import os

state = 'CA'
sessionYear = "2016-17"
prefix = "http://www.ebudget.ca.gov/" + sessionYear + '/'
pdfDir = "pdfs"
pdfVersion = 'A'    # A for Proposed, B for Revised, C for Enacted, 
                    #   D-Z for Other


        ############ INSERT STATEMENTS #############
QI_BILL = '''INSERT INTO Bill (bid, type, number, billState, status, 
                               house, session, sessionYear, state) 
             VALUES (%s, \'BUD\', %s, '', '', \'Governor\', 0, %s, %s)'''

QI_BILL_VERSION = '''INSERT INTO BillVersion (
                         vid, bid, date, billState, subject, appropriation,
                         substantive_changes, title, digest, text, state)
                     VALUES (%s, %s, NULL, \'Chaptered\', %s, NULL,
                             NULL, %s, NULL, NULL, %s)'''

        ############ SELECT STATEMENTS #############
QS_BILL = '''SELECT * 
             FROM Bill
             WHERE bid = %s'''

QS_BILL_VERSION = '''SELECT *
                     FROM BillVersion
                     WHERE vid = %s'''


'''
Downloads the pdf associated with a given budget line item,
   placing it in the "pdfs" directory if successful.
Prints a message to stdout indicating the result of the attempted download.

|url|: URL of the pdf to download
'''
def download_pdf(url, pdfName):
    returnCode = subprocess.call(
            "wget -q -t 5 -O {0} {1}".format(pdfDir + '/' + pdfName, url),
            shell=True)
    if returnCode:
        print "Attempted to download {0}, got error-code {1}".format(url, returnCode)
    else:
        print "Downloaded {0} successfully, named {1}".format(url, pdfName)

'''
Scrapes the url of the pdf linked to a given line item so it
   can be downloaded to a local directory

|suffix|: Endpoint to follow for getting the pdf

Returns a partial URL that can be used to download the pdf
'''
def get_pdf_url(suffix):
    try:
        html = urllib2.urlopen(prefix + suffix).read()
        pat = ('<A href=\'/2016-17/(.*?)\' class=\'blueLink\' '
               '>.*?</A><span class="content">')

        return re.search(pat, html).group(1)

    except urllib2.HTTPError as e:
        print '{0}: {1}'.format(prefix + suffix, e)

'''
Checks to see if a given line item is already in the BillVersion table,
    inserting it if not.

|bid|: budget item ID
|title|: title of the line item
|itemNum|: line item number
|cursor|: DB cursor
'''
def insert_bill_version(bid, title, itemNum, cursor):
    cursor.execute(QS_BILL_VERSION, (bid,))
    if cursor.rowcount == 0:
        print 'inserting {0} into BillVersion: {1} with title {2}\n'.format(bid, itemNum, title)
        cursor.execute(QI_BILL_VERSION, (bid + "_" + pdfVersion, bid, title, title, state))

'''
Checks to see if a given line item is already in the Bill table,
    inserting it if not.

|bid|: budget item ID
|itemNum|: line item number
|cursor|: DB cursor
'''
def insert_bill(bid, itemNum, cursor):
    cursor.execute(QS_BILL, (bid,))
    if cursor.rowcount == 0:
        print 'inserting item {0} into Bill as {1}'.format(itemNum, bid)
        cursor.execute(QI_BILL, (bid, itemNum,
                                 sessionYear.split('-')[0], state))
'''
Scrapes all of the budget line items for a given state agency (constructing the
    appropriate bID for a given line item based on state, session year,
    and item number) and then attempts to insert each into the DB.
Also downloads the pdf associated with a given line item (if it exists).

|agencyURL|: The URL containing the list of line items for a given agency
|cursor|: DB cursor
'''
def insert_line_items(agencyURL, cursor):
    try:
        html = urllib2.urlopen(agencyURL).read()
        pat = ('<td class="content" headers="code">(\d*?)</td>\s*<td headers='
               '"department">\s*<A border=\'0\' href=\'../../(.*?)\' class='
               '\'blueLink\' >(.*?)</A>|<td class="content" headers="code">'
               '(\d*?)</td>\s*<td headers="department">\s*'
               '<span class="content">(.*?)</span>')
       
        for match in re.finditer(pat, html):
            code = match.group(1) if match.group(1) else match.group(4)
            title = match.group(3) if match.group(1) else match.group(5)
            suffix = match.group(2) if match.group(1) else None

            bid = "{0}_{1}20{2}BUD{3}".format(state, sessionYear.split('-')[0],
                                              sessionYear.split('-')[1], code)
            pdfURL = prefix + get_pdf_url(suffix) if suffix else None
            if pdfURL:
                pdfName = "{0}_{1}20{2}_{3}_{4}.pdf".format(
                    state, sessionYear.split('-')[0],
                    sessionYear.split('-')[1], code, pdfVersion)
#                download_pdf(pdfURL, pdfName)

            insert_bill(bid, code, cursor)
            insert_bill_version(bid, title, code, cursor)

    except urllib2.HTTPError as e:
        print '{0}: {1}'.format(agencyURL, e)

'''
Scrapes the URL for each state agency

Returns an iterator for the list of agency URLs
'''
def get_agency_urls():
    try:
        html = urllib2.urlopen(prefix + "agencies.html").read()
        pat = ('<td width="35%" headers="agency"><A border=\'0\' '
                'href=\'(.*?)\' class=\'blueLink\' >(.*?)</A>')
	return re.finditer(pat, html)

    except urllib2.HTTPError as e:
        print '{0}: {1}'.format(prefix + "agencies.html", e)


def main():
    if not os.path.exists(pdfDir):
        os.makedirs(pdfDir)
    with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2015Dec',
                         user='awsDB',
                         passwd='digitaldemocracy789',
                         charset='utf8') as cursor:
        for match in get_agency_urls():
            insert_line_items(prefix + match.group(1), cursor)


if __name__ == '__main__':
    main()

