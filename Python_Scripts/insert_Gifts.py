'''
File: insert_Gifts.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers Gift Data and puts it into DDDB2015.Gift
- Used once for the Insertion of all the Gifts
- Fills table:
	Gift (pid, schedule, sourceName, activity, city, cityState, value, giftDate, reimbursed, giftIncomeFlag, speechFlag, description)

Source:
- Gifts.txt

'''

import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_Gift = "INSERT INTO Gift (pid, schedule, sourceName, activity, city, cityState, value, giftDate, reimbursed, giftIncomeFlag, speechFlag, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn = db.cursor(buffered = True)

def getPid(cursor, info):
	info = info.split(' ')
	if(info[1] == "AD"):
		house = "Assembly"
	elif(info[1] == "SD"):
		house = "Senate"
	else:
		print "error!"
		return -1
	district = info[0]
	select_stmt = "SELECT pid FROM Term WHERE house = %(house)s AND district = %(district)s;"
	cursor.execute(select_stmt, {'house':house, 'district':district})
	if cursor.rowcount == 1:
		temp = cursor.fetchone()
		return temp[0]
	else:
		print "error more than one!"
		print cursor.rowcount
		return -1

def insert_Gift(cursor, pid, schedule, sourceName, activity, city, cityState, value, giftDate, reimbursed, giftIncomeFlag, speechFlag, description):	
	cursor.execute(query_insert_Gift, (pid, schedule, sourceName, activity, city, cityState, value, giftDate, reimbursed, giftIncomeFlag, speechFlag, description))

f = open("Gifts.txt", "r+")
for line in f:
	attributes = line.split('<>')
	pid = getPid(conn, attributes[0])
	if attributes[1] is "TRUE":
		schedule = "D"
	else:
		schedule = "E"
	sourceName = attributes[2]
	activity = attributes[3]
	city = attributes[4]
	cityState = attributes[5]
	value = attributes[6]
	if len(attributes[7].split('/')) == 3:
		dateArray = attributes[7].split('/')
		newArray = [dateArray[2], dateArray[0], dateArray[1]]
		giftDate = '-'.join(newArray)
	else:
		giftDate = '';
	if "X" in attributes[8]:
		print 'reimbusrsed'
		reimbursed = 1
	else:
		reimbursed = 0
	if attributes[9] is "Gift":
		print 'Gift'
		giftIncomeFlag = 1
	else:
		giftIncomeFlag = 0
	if attributes[10] is "X":
		print 'speech'
		speechFlag = 1
	else:
		speechFlag = 0
	description = attributes[11]
	if pid != -1:
		insert_Gift(conn, pid, schedule, sourceName, activity, city, cityState, value, giftDate, reimbursed, giftIncomeFlag, speechFlag, description)
db.commit()
db.close()
	
	