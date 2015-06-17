import mysql.connector
import re
import sys
import csv
import datetime
import codecs

query_get_random_legislator = "SELECT a.first, a.last FROM Person as a JOIN Legislator as b WHERE a.pid = b.pid AND RAND() <= 0.25 ORDER BY a.pid;"
query_get_random_people = "SELECT Person.first, Person.last FROM Person WHERE RAND() <= 0.25 ORDER BY Person.pid;"
query_get_random_bills = "SELECT Bill.bid FROM Bill WHERE RAND() <= 0.25;"
query_get_random_videos = "SELECT Video.youtubeId FROM Video ORDER BY RAND() LIMIT 5;"

def get_random_legislators(cursor):
	f = codecs.open('legislator.txt', encoding='utf-8', mode='w+')
	cursor.execute(query_get_random_legislator)
	for i in range(0,cursor.rowcount):
		result = cursor.fetchone();
		u = u"{0} {1}".format(unicode(result[0]), unicode(result[1]))
		print u
		f.write(u)
	f.close()

def get_random_people(cursor):
	f = codecs.open('persons.txt', encoding='utf-8', mode='w+')
	cursor.execute(query_get_random_people)
	for i in range(0,cursor.rowcount):
		result = cursor.fetchone();
		u = u"{0} {1}".format(unicode(result[0]), unicode(result[1]))
		print u
		f.write(u)
	f.close()

def get_random_bills(cursor):
	f = open('bills.txt', 'w')
	cursor.execute(query_get_random_bills)
	for i in range(0,cursor.rowcount):
		result = cursor.fetchone();
		print "{0}".format(result[0])
		f.write("{0}\n".format(result[0]))
	f.close()

def get_random_videos(cursor):
	f = open('videos.txt', 'w')
	cursor.execute(query_get_random_videos)
	for i in range(0,cursor.rowcount):
		result = cursor.fetchone();
		print "{0}".format(result[0])
		f.write("{0}\n".format(result[0]))
	f.close()

db = mysql.connector.connect(user = 'root', db = 'DDDB2015', password = '')
dd = db.cursor(buffered = True)

try:
	get_random_legislators(dd)
	get_random_people(dd)
	get_random_bills(dd)
	get_random_videos(dd)
	
except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

db.close()