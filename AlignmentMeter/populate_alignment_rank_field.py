import os
import pymysql

def main():
    db = pymysql.connect(host='dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         #host='dev.digitaldemocracy.org',
                         # db=AndrewTest',
                         db='DDDB2016Aug',
                         user='scripts',
                         #user='dbMaster',
                         passwd=os.environ['DBMASTERPASSWORD'])

    cur = db.cursor()
    cur.execute("""
                UPDATE CombinedAlignmentScores
                SET rank=5
                """)
    cur.execute("""
                UPDATE CombinedAlignmentScores
                SET rank=4
                WHERE pid_house_party="nan_Senate_Republican"
                """)
    cur.execute("""
                UPDATE CombinedAlignmentScores
                SET rank=3
                WHERE pid_house_party="nan_Senate_Democrat"
                """)
    cur.execute("""
                UPDATE CombinedAlignmentScores
                SET rank=2
                WHERE pid_house_party="nan_Assembly_Republican"
                """)
    cur.execute("""
                UPDATE CombinedAlignmentScores
                SET rank=1
                WHERE pid_house_party="nan_Assembly_Democrat"
                """)
    cur.execute("""
                UPDATE CombinedAlignmentScores
                SET rank=0
                WHERE pid_house_party="nan_nan_nan"
                """)

    db.commit()
    db.close()

if __name__ == "__main__":
    main()
