import mysql.connector
import re
import sys
import csv
import datetime
import codecs

query_get_legislators = "SELECT a.first, a.last, a.pid from Person as a JOIN Legislator as b WHERE a.pid = b.pid;"

def get_random_legislators(cursor):
	f = codecs.open('legislators.txt', encoding='utf-8', mode='w+')
	f.write("{")
	cursor.execute(query_get_legislators)
	for i in range(0,cursor.rowcount):
		result = cursor.fetchone();
		u = u"{0}, {1}, {2}".format(unicode(result[0]), unicode(result[1]), unicode(result[2]))
		print u
		f.write("{" + u + "}")
		if(i != cursor.rowcount - 1):
			f.write(",")
	f.write("}")
	f.close()

db = mysql.connector.connect(user = 'root', db = 'DDDB2015', password = '')
dd = db.cursor(buffered = True)

try:
	get_random_legislators(dd)
	
except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

db.close()