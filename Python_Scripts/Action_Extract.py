'''
File: Action_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Inserts the Actions from the bill_history_tbl from capublic into DDDB2015Apr.Action
- This script runs under the update script
- Fills table:
	Action (bid, date, text)

Sources:
- Leginfo (capublic)
	- Pubinfo_2015.zip
	- Pubinfo_Mon.zip
	- Pubinfo_Tue.zip
	- Pubinfo_Wed.zip
	- Pubinfo_Thu.zip
	- Pubinfo_Fri.zip
	- Pubinfo_Sat.zip

-capublic
	- bill_history_tbl
'''

import re
import sys
import time
import mysql.connector
from pprint import pprint
from urllib import urlopen

# Insert statements that are used
query_insert_Action = "INSERT INTO Action (bid, date, text) VALUES (%s, %s, %s)";

#the connectiosn to the database
db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
TheirConnection = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
OurConnection = db2.cursor(buffered = True)

#inserts the Action
def insert_Action(cursor, bid, date, text):
	#checks if the Action is already in the database
	select_stmt = "SELECT bid from Action where bid = %(bid)s AND date = %(date)s"
	cursor.execute(select_stmt, {'bid':bid, 'date':date})
	#If the specified Action is not in the database, insert it
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_Action, (bid, date, text))	
	#Otherwise pass
	else:
		#print "Motion mid = {0}, date = {1}, text = {2} Already Exists!".format(bid, date, text)
		pass

def main():
	#Get all of the actions from capublic
	select_stmt = "SELECT * FROM bill_history_tbl"
	TheirConnection.execute(select_stmt)
	for i in range(0, TheirConnection.rowcount):
		#Try to insert the actions one at a time, so an exception will not stop the whole process
		try:
			tuple = TheirConnection.fetchone()
			if tuple:
				bid = tuple[0];
				date = tuple[2];
				text = tuple[3];
				if(bid):
					insert_Action(OurConnection, bid, date, text)
			db2.commit()
		#If there is an error, print the error and exit
		except:
			db2.rollback()
			print 'error!', sys.exc_info()[0], sys.exc_info()[1]
			exit()	
	db.close();
	db2.close();	

if __name__ == "__main__":
	main()
