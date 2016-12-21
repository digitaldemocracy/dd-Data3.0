'''
File: export_queries.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 11/08/2016
Last Modified: 11/08/2016

DDDB Data Analytical Query List:
  https://docs.google.com/document/d/1iDqjmmq7vTz7_y3Uv8vfbijq6BovAAH8NCc24lenrVY/edit?usp=sharing

  If link doesn't work then ask Alex Dekhtyar for permission to view it.

Runtime:
    0:53:02.840626
'''

import pymysql
import pandas as pd
from datetime import datetime
import csv

CONN_INFO = {'host': 'dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}

'''
VOTE STATS: For any given legislator, what is the percent of bills on which they vote ‘Aye’, 
percent of bills on which they vote ‘No’, and percent of bills on which they abstain 
(all locations, all bills, all votes)
'''
QS_VOTE_STATS_ALL_LOC = '''SELECT pid, total, aye, noe, abst, aye/total as aye_percent, noe/total as noe_percent, abst/total as abs_percent 
                          FROM
                          (SELECT COUNT(*) as total, pid
                          FROM LegVotes 
                          WHERE pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)) t,
                          (SELECT COUNT(*) as aye
                          FROM LegVotes
                          WHERE pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)
                          AND result = 'AYE') y,
                          (SELECT COUNT(*) as noe
                          FROM LegVotes
                          WHERE pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)
                          AND result = 'NOE') n,
                          (SELECT COUNT(*) as abst
                          FROM LegVotes
                          WHERE pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)
                          AND result = 'ABS') a'''

'''
VOTE STATS: For any given legislator, what is the percent of bills on which they vote ‘Aye’,
and the percent of bills on which they vote ‘No’, and percent of bills on which they abstain 
per committee on which the legislator sits.
'''
QS_VOTE_STATS_COM = '''SELECT total, aye, noe, abst, aye/total as aye_percent, noe/total as noe_percent, abst/total as abs_percent
                          FROM
                          (SELECT COUNT(*) as total, pid
                          FROM LegVotes
                          WHERE voteId in (SELECT DISTINCT voteId 
                              FROM BillVoteSummary 
                              WHERE cid = %(cid)s)
                          AND pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)) t,
                          (SELECT COUNT(*) as aye
                          FROM LegVotes
                          WHERE voteId in (SELECT DISTINCT voteId 
                              FROM BillVoteSummary 
                              WHERE cid = %(cid)s)
                          AND pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)
                          AND result = "AYE") y,
                          (SELECT COUNT(*) as noe
                          FROM LegVotes
                          WHERE voteId in (SELECT DISTINCT voteId 
                              FROM BillVoteSummary 
                              WHERE cid = %(cid)s)
                          AND pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)
                          AND result = "NOE") n,
                          (SELECT COUNT(*) as abst
                          FROM LegVotes
                          WHERE voteId in (SELECT DISTINCT voteId 
                              FROM BillVoteSummary 
                              WHERE cid = %(cid)s)
                          AND pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)
                          AND result = "ABS") a'''

'''
ABSENTEEISM: Surface all instances in which a legislator has registered no votes (so abstained/not voting/no vote recorded) 
on all bills heard in a committee or floor session, and has registered no utterances. The goal here is to get at absenteeism.
'''
QS_VOTE_ABS = '''SELECT DISTINCT xx.bid, xx.hid, DATE(xx.VoteDate) as date
                FROM (SELECT DISTINCT bvd.pid, bvs.voteId, bvd.result, bvs.bid, bvs.VoteDate, h.hid, p.first, p.middle, p.last, bvd.state
                    FROM PassingVotes bvs
                        JOIN Motion m
                        ON bvs.mid = m.mid
                        JOIN BillVoteDetail bvd
                        ON bvd.voteId = bvs.voteId
                        JOIN Committee c
                        ON c.cid = bvs.cid
                        JOIN Hearing h
                        ON date(bvs.VoteDate) = h.date
                        JOIN CommitteeHearings ch
                        ON ch.hid = h.hid
                        AND ch.cid = bvs.cid
                        JOIN Person p
                        ON p.pid = bvd.pid
                        JOIN BillDiscussion bd
                        ON bd.bid = bvs.bid
                        AND bd.hid = h.hid
                    WHERE bvd.pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)
                    AND bvd.result='ABS') xx
                WHERE xx.hid NOT IN (SELECT DISTINCT vi.hid
                    FROM Utterance u JOIN Video vi ON vi.vid = u.vid JOIN
                        (SELECT DISTINCT h.hid
                        FROM PassingVotes bvs
                            JOIN Motion m
                            ON bvs.mid = m.mid
                            JOIN BillVoteDetail bvd
                            ON bvd.voteId = bvs.voteId
                            JOIN Committee c
                            ON c.cid = bvs.cid
                            JOIN Hearing h
                            ON date(bvs.VoteDate) = h.date
                            JOIN CommitteeHearings ch
                            ON ch.hid = h.hid
                            AND ch.cid = bvs.cid
                            JOIN Person p
                            ON p.pid = bvd.pid
                            JOIN BillDiscussion bd
                            ON bd.bid = bvs.bid
                            AND bd.hid = h.hid
                        WHERE bvd.pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s)
                        AND bvd.result='ABS') x ON x.hid = vi.hid
                    WHERE u.pid = (SELECT p.pid FROM Person p, Legislator l WHERE p.pid = l.pid AND first = %(first)s AND last = %(last)s));'''

'''
PARTICIPATION: Rank all 120 legislators by who, on average, makes the longest uninterrupted statements on the floor.
'''
QS_LONG_STMTS = '''SELECT s.pid, p.first, p.last, AVG(totalWords) as avg_statements_words, AVG(totalTime) as avg_statements_time, COUNT(*) as totalStatements
                  FROM Statement s, CommitteeHearings ch, Person p, Legislator l
                  WHERE s.hid = ch.hid AND ch.cid IN (SELECT cid FROM Committee WHERE name like '%Floor%')
                  AND p.pid = s.pid AND p.pid = l.pid AND l.state = "CA"
                  GROUP BY s.pid
                  ORDER BY avg_statements_words DESC'''

'''
PARTICIPATION: For each committee, rank all committee members by who, on average, makes the longest uninterrupted statements.
'''
QS_LONG_STMTS_COM = '''SELECT s.pid, p.first, p.last, AVG(totalWords) as avg_statements_words, AVG(totalTime) as avg_statements_time, COUNT(*) as totalStatements
                      FROM Statement s, CommitteeHearings ch, Person p, servesOn so
                      WHERE s.hid = ch.hid AND p.pid = s.pid AND so.pid = s.pid
                      AND ch.cid = so.cid AND so.cid = %s
                      GROUP BY s.pid
                      ORDER BY avg_statements_words DESC'''

QS_ALL_COMMITTEE = '''SELECT cid 
                      FROM Committee 
                      WHERE state="CA"'''

QS_PERSON = '''SELECT p.first, p.last, l.pid
              FROM Person p, Legislator l
              WHERE p.pid = l.pid'''

QS_SERVESON = '''SELECT DISTINCT cid
                FROM servesOn
                WHERE pid = %s'''

QS_COMMITTEE = '''SELECT name
                  FROM Committee
                  WHERE cid = %s'''

'''
This function fetches all the legislator votes for only passing votes
and creates a temporary table for some of the queries to use. 
'''
def fetch_leg_votes(dddb, cnxn):
  stmt = """CREATE OR REPLACE VIEW DoPassVotes
              AS
              SELECT bid,
                  voteId,
                  m.mid,
                  b.cid,
                  VoteDate,
                  ayes,
                  naes,
                  abstain,
                  result,
                  c.house,
                  c.type,
                  CASE
                      WHEN result = "(PASS)" THEN 1
                      ELSE 0
                  END AS outcome
              FROM BillVoteSummary b
                  JOIN Motion m
                  ON b.mid = m.mid
                  JOIN Committee c
                  ON b.cid = c.cid
              WHERE m.doPass = 1"""
  dddb.execute(stmt)

  stmt = """CREATE OR REPLACE VIEW FloorVotes
              AS
              SELECT b.bid,
                  b.voteId,
                  b.mid,
                  b.cid,
                  b.VoteDate,
                  b.ayes,
                  b.naes,
                  b.abstain,
                  b.result,
                  c.house,
                  c.type,
                  CASE
                      WHEN result = "(PASS)" THEN 1
                      ELSE 0
                  END AS outcome
              FROM BillVoteSummary b
                  JOIN Committee c
                  ON b.cid = c.cid
                  JOIN Motion m
                  on b.mid = m.mid
              WHERE m.text like '%reading%'"""
  dddb.execute(stmt)

  stmt = """CREATE OR REPLACE VIEW PassingVotes
              AS
              SELECT *
              FROM DoPassVotes
              UNION
              SELECT *
              FROM FloorVotes;"""
  dddb.execute(stmt)

  query = """SELECT DISTINCT bvd.pid, bvs.voteId, bvd.result, bvs.bid, bvs.cid, DATE(bvs.VoteDate) as date,
                h.hid, p.first, p.middle, p.last, bvd.state
             FROM PassingVotes bvs
                 JOIN Motion m
                 ON bvs.mid = m.mid
                 JOIN BillVoteDetail bvd
                 ON bvd.voteId = bvs.voteId
                 JOIN Committee c
                 ON c.cid = bvs.cid
                 JOIN Hearing h
                 ON date(bvs.VoteDate) = h.date
                 JOIN CommitteeHearings ch
                 ON ch.hid = h.hid
                  AND ch.cid = bvs.cid
                 JOIN Person p
                 ON p.pid = bvd.pid
                 JOIN BillDiscussion bd
                 ON bd.bid = bvs.bid
                   AND bd.hid = h.hid"""

  #Create the LegVotes table just for running the above queries.
  leg_votes_df = pd.read_sql(query, cnxn)
  leg_votes_df.to_sql('LegVotes', cnxn, flavor='mysql', if_exists='replace', index=False)

  #Index the table to make it faster
  stmt = '''ALTER TABLE LegVotes
            ADD INDEX pid_idx (pid)'''
  dddb.execute(stmt)

'''
Removes the temporary mysql views created and drops the LegVote table.
'''
def remove_temps(dddb):
  dddb.execute('DROP VIEW DoPassVotes')
  dddb.execute('DROP VIEW FloorVotes')
  dddb.execute('DROP VIEW PassingVotes')
  dddb.execute('DROP TABLE LegVotes')

'''
This function is used for running the Vote Stats query for all locations.
'''
def vote_stats(dddb, output, names):
  query = QS_VOTE_STATS_ALL_LOC
  #Create list of dictionaries of legislator names
  name_dicts = [{'first':name[0], 'last':name[1]} for name in names]
  #Write the header (table column names) to csv
  dddb.execute(query, name_dicts[0])
  desc = ['first', 'last']
  desc.extend([i[0] for i in dddb.description])
  output.writerow(desc)

  #Per legislator get their vote stats for all locations
  for d in name_dicts:
    try:
      dddb.execute(query, d)
      if dddb.rowcount > 0:
        row = [d['first'], d['last']]
        row.extend( dddb.fetchall()[0] )
        #Exclude vote stats where the legislator has no votes
        if row[3] != 0:
          output.writerow(row)
    except:
      print(query%d)
      #exit(1)

'''
This function collects all the committees a legislator serves on.
'''
def get_leg_committees(dddb, pid):
  comms = []
  dddb.execute(QS_SERVESON, pid)
  if dddb.rowcount > 0:
    comms = [tup[0] for tup in dddb.fetchall()]

  return comms

'''
This function gets all the committee names in California.
'''
def get_committee_name(dddb, cid):
  name = ''
  dddb.execute(QS_COMMITTEE, cid)
  if dddb.rowcount > 0:
    name = dddb.fetchone()[0]

  return name

'''
This function is used for running the Vote Stats query
per committee legislator sits on.
'''
def vote_stats_com(dddb, output, names):
  #name_dicts = [{'first':name[0], 'last':name[1], 'cid':get_leg_committees(dddb,name[2])} for name in names]
  #Create list of dictionary entries that hold legislator name
  #and committee ID
  name_dicts = []
  for name in names:
    for cid in get_leg_committees(dddb, name[2]):
      name_dicts.append({'first':name[0], 'last':name[1], 'cid':cid})

  #Write the header (table column names) to csv
  dddb.execute(QS_VOTE_STATS_COM, name_dicts[0])
  desc = ['first', 'last', 'cid', 'name']
  desc.extend([i[0] for i in dddb.description])
  output.writerow(desc)

  #Per legislator and his committee write the vote stats to csv
  for d in name_dicts:
    #Get the committee name
    comm_name = get_committee_name(dddb, d['cid'])
    dddb.execute(QS_VOTE_STATS_COM, d)
    if dddb.rowcount > 0:
      row = [d['first'], d['last'], d['cid'], comm_name]
      row.extend( dddb.fetchall()[0] )
      #output.writerow(row)
      #Exclude vote stats where the legislator has no votes
      if row[4] != 0:
          output.writerow(row)

'''
This function is used for running the Absenteeism query
for all the legislators who abstained and spoke no words in hearing.
'''
def vote_abs(dddb, output, names):
  #Create a list of dictionaries of legislator names
  name_dicts = [{'first':name[0], 'last':name[1]} for name in names]
  #Write the header (table column names) to csv
  dddb.execute(QS_VOTE_ABS, name_dicts[0])
  desc = ['first', 'last', 'total absent']
  desc.extend([i[0] for i in dddb.description])
  output.writerow(desc)

  #Per legislator write the total absents and the
  #hearing and bid they were absent for
  for d in name_dicts:
    dddb.execute(QS_VOTE_ABS, d)
    if dddb.rowcount > 0:
      #Write the name and total number of absents
      row = [d['first'], d['last'], dddb.rowcount]
      output.writerow(row)

      #Write all the bills, hearings, and dates they abstained from
      #and spoke no words in the hearing
      for fetch in dddb.fetchall():
        r = ['','','']
        r.extend(fetch)
        output.writerow(r)

'''
This function is used for running the Vote Participation query
for all the legislators who have spoken on the Floor.
Metric: Average statement based of total words spoken.
'''
def longest_statements(dddb, output):
  dddb.execute(QS_LONG_STMTS)
  #Write the header (table column names) to csv
  output.writerow([i[0] for i in dddb.description])

  #dddb.execute(QS_LONG_STMTS)
  #Write the results of the query to csv
  if dddb.rowcount > 0:
    for entry in dddb.fetchall():
      output.writerow(entry)

'''
This function is used for running the Vote Participation query
per committee.
Metric: Average statement based of total words spoken.
'''
def longest_statements_com(dddb, output):
  #Get the header (table column names) to write to csv
  dddb.execute(QS_LONG_STMTS_COM, '452')
  desc = ['cid', 'committee name']
  desc.extend([i[0] for i in dddb.description])
  output.writerow(desc)

  #Get all cids of the committees in California
  dddb.execute(QS_ALL_COMMITTEE)
  #Run through each committee and rank the legislators based
  #on longest uninterrupted statements
  for com in dddb.fetchall():
    dddb.execute(QS_LONG_STMTS_COM, (com[0]))
    if dddb.rowcount > 0:
      #Gets the committee name and cid to write to csv
      name = get_committee_name(dddb, com[0])
      row = [com[0], name]
      output.writerow(row)

      #Execute query on that committee and write results to csv
      dddb.execute(QS_LONG_STMTS_COM, (com[0]))
      for fetch in dddb.fetchall():
        r = ['', '']
        r.extend(fetch)
        output.writerow(r)

'''
This function gets the names and pid of all the legislators.
'''
def get_legislators_names(dddb):
  names = []
  dddb.execute(QS_PERSON)
  if dddb.rowcount > 0:
    names = [(name[0], name[1], name[2]) for name in dddb.fetchall()]
    
  return names

def main():
  #Get the start time of this script
  startTime = datetime.now()

  #Create DDDB connection
  cnxn = pymysql.connect(**CONN_INFO)
  dddb = cnxn.cursor()

  #Create temporary tables needed for some queries
  fetch_leg_votes(dddb, cnxn)

  first_last_names = get_legislators_names(dddb)

  #GET RESULTS AND OUTPUT TO CSV
  filename = 'output1.csv'
  output = csv.writer(open(filename, 'w'))

  question = 'VOTE STATS: For any given legislator, what is the percent of bills on which they vote Aye, \
percent of bills on which they vote No, and percent of bills on which they abstain \
(all locations, all bills, all votes)'
  output.writerow([question])
  output.writerow([])
  output.writerow([])

  vote_stats(dddb, output, first_last_names)
  print('Query 1 done..')

  filename = 'output2.csv'
  output = csv.writer(open(filename, 'w'))

  question = 'VOTE STATS: For any given legislator, what is the percent of bills on which they vote Aye, \
and the percent of bills on which they vote No, and percent of bills on which they abstain \
per committee on which the legislator sits.'
  output.writerow([question])
  output.writerow([])
  output.writerow([])

  vote_stats_com(dddb, output, first_last_names)
  print('Query 2 done..')

  filename = 'output3.csv'
  output = csv.writer(open(filename, 'w'))

  question = 'ABSENTEEISM: Surface all instances in which a legislator has registered no votes (so abstained/not voting/no vote recorded) \
on all bills heard in a committee or floor session, and has registered no utterances. The goal here is to get at absenteeism.'
  output.writerow([question])
  output.writerow([])
  output.writerow([])

  vote_abs(dddb, output, first_last_names)
  print('Query 3 done..')
  

  filename = 'output4.csv'
  output = csv.writer(open(filename, 'w'))

  question = 'PARTICIPATION: Rank all 120 legislators by who, on average, makes the longest uninterrupted statements on the floor.'
  output.writerow([question])
  output.writerow([])
  output.writerow([])

  longest_statements(dddb, output)
  print('Query 4 done..')

  filename = 'output5.csv'
  output = csv.writer(open(filename, 'w'))

  question = 'PARTICIPATION: For each committee, rank all committee members by who, on average, makes the longest uninterrupted statements.'
  output.writerow([question])
  output.writerow([])
  output.writerow([])

  longest_statements_com(dddb, output)
  print('Query 5 done..')

  remove_temps(dddb)

  #Close DDDB connection
  cnxn.commit()
  cnxn.close()

  #Print the total runtime of the script
  print(datetime.now() - startTime)

if __name__ == '__main__':
  main()