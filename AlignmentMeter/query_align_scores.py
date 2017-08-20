import MySQLdb
import csv
from progress.bar import Bar

def connect_db(stmt):

    db = MySQLdb.connect(host="dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com",
                         user="dbMaster",
                         passwd="BalmerPeak",
                         db="DDDB2016Aug")

    cur = db.cursor()
    cur.execute(stmt)
    db.close()

    return cur

def get_pids():
    stmt = """ SELECT DISTINCT pid from AlignmentScoresData """
    pids = []

    cur = connect_db(stmt)
    results = cur.fetchall()
    for pid in results:
        pids.append(pid[0])

    return pids

def get_oids():
    stmt = """ SELECT DISTINCT oid from AlignmentScoresData """
    oids = []

    cur = connect_db(stmt)
    results = cur.fetchall()
    for oid in results:
        oids.append(oid[0])

    return oids

def get_align_data(pid, oid):
    stmt = """ 
           SELECT abstain_vote, resolution, unanimous,
                  leg_last, leg_first, organization, bill, 
                  leg_alignment, leg_vote_date, 
                  org_alignment, date_of_org_alignment
           FROM AlignmentScoresData 
           WHERE pid = {0}
           AND oid = {1}
           """
    criteria = {'000' : [],
                '001' : [],
                '010' : [],
                '011' : [],
                '100' : [],
                '101' : [],
                '110' : [],
                '111' : [],
               }

    votes = ['000', '001', '010', '011', '100', '101', '110', '111']

    cur = connect_db(stmt.format(pid, oid))
    header = [i[0] for i in cur.description]
    results = cur.fetchall()
    for row in results:
        key = (str(row[0]) + str(row[1]) + str(row[2]))
        criteria[key].append(row)

    for vote in votes:
        flags = list(vote)
        filename = '{0}_{1}_{2}_{3}_{4}.csv'.format(pid, oid, flags[0], flags[1], flags[2])
        with open('align_data/' + filename, 'wb') as csvfile:
            w = csv.writer(csvfile)
            w.writerow(header)
            for key, value in criteria.iteritems():
                crit = list(key)
                if value and not(flags[0] == '1' and flags[0] == crit[0]) and not(flags[1] == '1' and flags[1] == crit[1]) and not(flags[2] == '1' and flags[2] == crit[2]):
                    for v in value:
                        w.writerow(v)

def main():
    pids = get_pids()
    oids = get_oids()

    ## TEST
    #pids = [1, 99, 68321]
    #oids = [-23, -9, -7]

    ## SIMPLE TEST
    #pids = [68321]
    #oids = [-7]


    bar = Bar('Processing', max=len(pids) * len(oids))
    for pid in pids:
        for oid in oids:
            get_align_data(pid, oid)
            bar.next()
    bar.finish()

if __name__ == "__main__":
    main()