'''
  Author: Miguel Aguilar
  Maintained By: Miguel Aguilar
  Date: 10/18/2016
  Last Updated: 10/18/2016
'''

import os, sys
import pymysql

CONN_INFO = {'host': 'dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}

#MySQL queries that will get all the oids for an organization under different names
Org_Query = {'California Chamber Of Commerce': 'SELECT * FROM Organizations WHERE name REGEXP "^(California|Cal).{0,2}Chamber.{0,5}Commerce$";',
            'California State Association Of Counties': 'SELECT * FROM Organizations WHERE name like "Cal%Counties%";',
            'California District Attorneys Association': 'SELECT * FROM Organizations WHERE name like "%Cal%District%Attorney%";',
            'Children Now':'SELECT * FROM Organizations WHERE name like "%Children%Now%";',
            'California Manufacturers And Technology Association':'SELECT * FROM Organizations WHERE name like "%Cal%Manufacturer%Technology%Association" or name like "CMTA";',
            'Howard Jarvis Taxpayers Association':'SELECT * FROM Organizations WHERE name like "%Howard%Jarvis%Taxpayer%Association";',
            'California Farm Bureau Federation':'SELECT * FROM Organizations WHERE name like "%Cal%Farm%Bureau%Federation" or name like "%Cal%Farm%Bureau";',
            'California Catholic Conference':'SELECT * FROM Organizations WHERE name like "%Cal%Catholic%";',
            'Association Of California Water Agencies':'SELECT * FROM Organizations WHERE name like "%Ass%Cal%Water%Agen%";',
            'California Building Industry Association':'SELECT * FROM Organizations WHERE name REGEXP "^California.{0,2}Building.{0,2}Industry.{0,2}(Ass.{0,5}|Association)$";',
            'California Labor Federation':'SELECT * FROM Organizations WHERE name REGEXP "^(California|Cal).{0,2}Labor.{0,4}Federation.{0,10}$";',
            'Sierra Club California':'SELECT * FROM Organizations WHERE name like "Sierra%Club" or name like "Sierra%Club%California";',
            'California Teachers Association':'SELECT * FROM Organizations WHERE name REGEXP "^(California|Cal).{0,2}Teacher.{0,3}(Assn|Assoc|Association).{0,2}$" or name like "CTA";',
            'California Immigrant Policy Center':'SELECT * FROM Organizations WHERE name REGEXP "^(California|Cal).{0,2}Immigrant.{0,2}Policy.{0,8}$";',
            'American Civil Liberties Union of California':'SELECT * FROM Organizations WHERE name REGEXP "^American.{0,2}Civil.{0,2}Liberties.{0,2}Union.{0,4}(California)?$";',
            'Consumer Attorneys Of California':'SELECT * FROM Organizations WHERE name REGEXP "^Consumer.{0,2}Attorneys.{0,4}(California|Cal|Ca)$" or name like "Consumer%Attorneys";',
            'Disability Rights California':'SELECT * FROM Organizations WHERE name like "Disability%Rights%California";',
            'California Medical Association':'SELECT * FROM Organizations WHERE name REGEXP "^(California|Cal|Ca).{0,2}Medical.{0,2}(Association|Associate|Assoc|Assn).{0,2}(Inc.)?$";',
            'Western Center on Law and Poverty':'SELECT * FROM Organizations WHERE name like "%Western%Law%Poverty%";',
            'American Federation of State, County and Municipal Employees':'SELECT * FROM Organizations WHERE name REGEXP "^American.{0,2}(Federation|Confederation|Federatio).{0,2}Of.{0,2}State.{0,6}County.{0,6}Municipal.{0,2}(Home.{0,2})?Employees(.{0,3}((California|CA) People|AFL-CIO))?$";'}

QS_ORGCONCEPT = '''SELECT oid
                FROM OrgConcept
                WHERE name = %s'''

QS_ORGCONCEPT_AFFILIATION = '''SELECT *
                              FROM OrgConceptAffiliation
                              WHERE new_oid = %s 
                              and old_oid = %s'''

QS_ORGCONCEPT_MIN = '''SELECT MIN(oid)
                      FROM OrgConcept'''

QI_ORGCONCEPT = '''INSERT INTO OrgConcept
                    (oid, name)
                    VALUES
                    (%s, %s)'''

QI_ORGCONCEPT_AFFILIATION = '''INSERT INTO OrgConceptAffiliation
                                (new_oid, old_oid)
                                VALUES
                                (%s, %s)'''

'''
  1) Read all the Org names in the text file and insert into OrgConcept
  2) Then for each Org, query the db and get all the similar oids
  3) Insert those oids into OrgConceptAffiliation under a meta Org
'''

'''
This function gets the minimum oid in the Org Concept table.
In order words gets the latest org concept inserted since its
oid decreases (negative numbers). 
'''
def get_min_oid(dddb):
  try:
    dddb.execute(QS_ORGCONCEPT_MIN)
  except MySQLError as e:
    print(e)
  return dddb.fetchone()[0]

'''
This function checks to see if the org concept affiliation exists
in the DB table already, if not then insert it.
'''
def insert_org_concept_affiliation(dddb, org_name):
  try:
    #Query that gets all the oids for the same organization under varying names
    query = Org_Query[org_name]
    dddb.execute(query)
    #Get a list of the oids of the same org
    oids = [x[0] for x in dddb.fetchall()]
    dddb.execute(QS_ORGCONCEPT, org_name)
    if dddb.rowcount > 0:
      new_oid = dddb.fetchone()[0]
      #Insert all the org oids and match them with the new oid for that org
      for oid in oids:
        dddb.execute(QS_ORGCONCEPT_AFFILIATION, (new_oid, oid))
        if dddb.rowcount == 0:
          #print(QI_ORGCONCEPT_AFFILIATION%(new_oid, oid))
          dddb.execute(QI_ORGCONCEPT_AFFILIATION, (new_oid, oid))
  except pymysql.Error as e:
    print(e)

'''
This function checks to see if the org concept exists in the
DB already, if not then insert it.
'''
def insert_org_concept(dddb, org_name):
  #Get the current min oid which is the latest entry id
  min_oid = get_min_oid(dddb)
  try:
    dddb.execute(QS_ORGCONCEPT, (org_name))
    if dddb.rowcount == 0:
      #print(QI_ORGCONCEPT%(str(int(min_oid)-1), org_name))
      dddb.execute(QI_ORGCONCEPT, (str(int(min_oid)-1), org_name))
  except pymysql.Error as e:
    print(e)

def main():
  #Usage requires a text file to be passed in
  #Text file must include name of the organizations
  #seperated by a new line
  if len(sys.argv) > 2 or len(sys.argv) < 2:
    print('Usage: merge_orgs.py [filename]')
    exit(1)

  #Open text file with organization names
  with open(sys.argv[1], 'r') as textfile:
    cnxn = pymysql.connect(**CONN_INFO)
    dddb = cnxn.cursor()
    
    #Read every name per line and insert into
    #org concepts and org affiliations tables
    for org_name in textfile.readlines():
      if len(org_name) > 1:
        #Clean name to assure every name has a standard form
        clean_name = org_name.strip()
        insert_org_concept(dddb, clean_name)
        insert_org_concept_affiliation(dddb, clean_name)

    cnxn.commit()
    cnxn.close()

if __name__ == '__main__':
  main()