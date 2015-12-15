import requests
import MySQLdb

def call_senate_api(restCall, year, house, offset):
	if house != "":
		house = "/" + house
	url = "http://legislation.nysenate.gov/api/3/" + restCall + "/" + str(year) + house + "?full=true&limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset=" + str(offset)
	r = requests.get(url)
	out = r.json()
	return out["result"]["items"]


def get_senators_api(year):
	senators = call_senate_api("members", year, "", 0)
	ret_sens = list()
	for senator in senators:
		sen = dict()
		name = senator['fullName'].split() 
		sen['house'] = senator['chamber'].title()
		sen['last'] = name[len(name) - 1]
		sen['state'] = "NY"
		sen['year'] = str(year)
			
		sen['first'] = name[0]
		for x in range(1, len(name) - 1):
			sen['first'] = sen['first'] + " " + name[x]
		sen['district'] = senator['districtCode']
		sen['image'] = senator['imgName']
		if sen['image'] is None:
			sen['image'] = ''
		ret_sens.append(sen)
	return ret_sens		

def add_senators_db(year, dddb):
	senators = get_senators_api(year)
	x = 0
	for senator in senators:
	#senator = senators[0]
		insert_stmt = '''INSERT INTO Person
						(last, first, image)
						VALUES
						(%(last)s, %(first)s, %(image)s);
						'''
		dddb.execute(insert_stmt, senator)
		print (insert_stmt % senator)
		pid = dddb.lastrowid
		senator['pid'] = pid
		insert_stmt = '''INSERT INTO Legislator
						(pid, state)
						VALUES
						(%(pid)s, %(state)s);
						'''
		dddb.execute(insert_stmt, senator)
		insert_stmt = '''INSERT INTO Term
						(pid, year, house, state, district)
						VALUES
						(%(pid)s, %(year)s, %(house)s, %(state)s, %(district)s);
						'''
		dddb.execute(insert_stmt, senator)
		x = x + 1

	print str(x) + " records added" 


def main():
	dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
						user='awsDB',
						db='JohnTest',
						port=3306,
						passwd='digitaldemocracy789')
	dddb = dddb_conn.cursor()
	dddb_conn.autocommit(True)
	add_senators_db(2015, dddb)
	dddb_conn.close()
	
main()

	
	

