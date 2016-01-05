from lxml import html
import requests
import MySQLdb

def get_last_cid_db(dddb):
	select_comm = '''SELECT cid FROM Committee
					ORDER BY cid DESC
					LIMIT 1
                  '''
	dddb.execute(select_comm)
	
	query = dddb.fetchone();
	return query[0]

def clean_name(name):
    bad = ['Jr','Sr','II','III', 'IV']
    name = name.replace(',', '')
    name = name.replace('.', '')
    name_arr = name.split()                     
    for word in name_arr:
        if len(word) <= 1 or  word in bad:
            name_arr.remove(word)
    first = name_arr[0]
    last = name_arr[1]
    for x in range(2, len(name_arr)):
        last = last + ' ' + name_arr[x]    
    return (first, last)
    
def get_committees_html(year):
    page = requests.get('http://assembly.state.ny.us/comm/')
    tree = html.fromstring(page.content)

    categories_html = tree.xpath('//*[@id="sitelinks"]/span//text()')
    ret_comms = list()
    committees = dict()
    x = 1
    for category in categories_html:
        committees_html = tree.xpath('//*[@id="sitelinks"]//ul['+str(x)+']//li/strong/text()')        
        y = 1
        for comm in committees_html:                    
            link = tree.xpath('//*[@id="sitelinks"]//ul['+str(x)+']//li['+str(y)+']/a[contains(@href,"mem")]/@href')
            if len(link) > 0:
                strip_link = link[0][0:len(link[0]) - 1]
            
                link = 'http://assembly.state.ny.us/comm/' + strip_link
                committee = dict()
            
                committee['members'] = list()
                
                member_page = requests.get(link)
                member_tree = html.fromstring(member_page.content)
                
                members_html = member_tree.xpath('//*[@id="sitelinks"]/span//li/a//text()')
                
                for mem in members_html:                    
                    sen = dict()
                    name = clean_name(mem)                        
                    
                    sen['last'] = name[1]        
                    sen['first'] = name[0]                                        
                    sen['year'] = str(year)
                    sen['house'] = "Assembly"
                    sen['state'] = "NY"
                    committee['members'].append(sen)

                committee['name'] = comm
                committee['type'] = category
                committee['house'] = "Assembly"
                committee['state'] = "NY"
            ret_comms.append(committee)    
            y = y + 1
        x = x + 1
           # print members_html
    return ret_comms 
        
        

def add_committees_db(year, dddb):
    committees = get_committees_html(year)

    for committee in committees:
        cid = get_last_cid_db(dddb) + 1
        committee['cid'] = str(cid)
        insert_stmt = '''INSERT INTO Committee
                        (cid, house, name, type, state)
                        VALUES
                        (%(cid)s, %(house)s, %(name)s, %(type)s, %(state)s);
                        '''
        print (insert_stmt % committee)
        dddb.execute(insert_stmt, committee)
    
        x = 0
        if len(committee['members']) > 0:
            for member in committee['members']:
                member['pid'] = get_pid_db(member, dddb)
                member['cid'] = committee['cid']
                insert_stmt = '''INSERT INTO servesOn
                            (pid, year, house, cid, state)
                            VALUES
                            (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s);
                            '''
                if member['pid'] != "bad":
                    #print (insert_stmt % member)
                    dddb.execute(insert_stmt, member)
                x = x + 1
      
                        
    
def get_pid_db(person, dddb):
    select_person = '''SELECT * 
                       FROM Person p, Legislator l
                       WHERE first = %(first)s AND last = %(last)s AND state = %(state)s
                       AND p.pid = l.pid'''                                           
    
    try:
        dddb.execute(select_person, person)
        query = dddb.fetchone();
        return query[0]
    except:
        print "Error", (select_person %  person)
        return "bad"

    
def main():
    dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                    user='awsDB',
                    db='JohnTest',
                    port=3306,
                    passwd='digitaldemocracy789')
    dddb = dddb_conn.cursor()
    dddb_conn.autocommit(True)

    add_committees_db(2015, dddb)

    dddb_conn.close()

main()