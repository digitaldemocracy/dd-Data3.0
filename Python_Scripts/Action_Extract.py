import re
import sys
import time
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_Action = "INSERT INTO Action (bid, date, text) VALUES (%s, %s, %s)";

db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn2 = db2.cursor(buffered = True)

def insert_Action(cursor, bid, date, text):
	select_stmt = "SELECT bid from Action where bid = %(bid)s AND date = %(date)s"
	cursor.execute(select_stmt, {'bid':bid, 'date':date})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_Action, (bid, date, text))	
	else:
		#print "Motion mid = {0}, date = {1}, text = {2} Already Exists!".format(bid, date, text)
		pass

select_count = "SELECT COUNT(*) FROM bill_history_tbl"
conn.execute(select_count)
temp = conn.fetchone()
a = temp[0]
print a
select_stmt = "SELECT * FROM bill_history_tbl"
conn.execute(select_stmt)
for i in range(0, a):
	try:
		temp = conn.fetchone()
		if temp:
			bid = temp[0];
			date = temp[2];
			text = temp[3];
			if(bid):
				insert_Action(conn2, bid, date, text)
		db2.commit()

	except:
		db2.rollback()
		print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		exit()

db.close();
db2.close();
