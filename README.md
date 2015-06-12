dd-Data3.0
==========

Scripts used for the database DDDB2015Apr.

updateData.sh
author: Daniel Mangin
date: 6/11/2015
Description: used to make a full update to DDDB2015Apr. Runs:
	- legislator_migrate.py
	- Cal-Access-Accessor.py
	- Bill_Extract.py
	- billparse.py
	- Author_Extract.py
	- Get_Committees_Web.py
	- Motion_Extract.py
	- Vote_Extract.py
	- Get_Districts.py
	- Action_Extract.py
Note: Each of the files must have the correct database connection for this to work properly.

-- file: DB-clear.sql
-- author: Daniel Mangin
-- date: 6/11/2015
-- Description: Used to drop all of the tables in the current database
-- note: this will only work on the currently used database

-- file: DB-setup.sql
-- author: Daniel Mangin
-- date: 6/11/2015
-- Description: Used to create all of the tables for Digital Democracy
-- note: this will only work on the currently used database

-- file: DB-update-Test-DB.sql
-- author: Daniel Mangin
-- date: 6/11/2015
-- Description: Used to turn DDDB2015AprTest into an exact copy of DDDB2015Apr

All other files are obselete. They were used for something temporary.

updateScripts/

===========

file: cleanCapublic.sh
author: Daniel Mangin
date: 6/11/2015
Description: Deletes all of capublic to avoid duplicate data. Used in opengov_load.sh

file: opengov_load.sh
author: Daniel Mangin
date: 6/11/2015
Description: Grabs the full pubinfo_2015.zip and recreates capublic. Is a full update of capublic and runs cleanCapublic.sh to remove all current data.
Notes: 
- Extracts data to leginfo_load
- Runs every Sunday at 10:30 PM
Sources:
- Leginfo
	- pubinfo_2015.zip

file: opengov_load_Mon.sh
author: Daniel Mangin
date: 6/11/2015
Description: Grabs the pubinfo_Mon.zip and adds the daily files to capublic.
Note: 
- Extracts data to leginfo_load_Mon
- Runs every Monday at 10:00 PM
Sources:
- Leginfo
	- pubinfo_Mon.zip

file: opengov_load_Tue.sh
author: Daniel Mangin
date: 6/11/2015
Description: Grabs the pubinfo_Tue.zip and adds the daily files to capublic.
Note: 
- Extracts data to leginfo_load_Tue
- Runs every Tuesday at 10:00 PM
Sources:
- Leginfo
	- pubinfo_Tue.zip

file: opengov_load_Wed.sh
author: Daniel Mangin
date: 6/11/2015
Description: Grabs the pubinfo_Wed.zip and adds the daily files to capublic.
Note: 
- Extracts data to leginfo_load_Wed
- Runs every Wednesday at 10:00 PM
Sources:
- Leginfo
	- pubinfo_Wed.zip

file: opengov_load_Thu.sh
author: Daniel Mangin
date: 6/11/2015
Description: Grabs the pubinfo_Thu.zip and adds the daily files to capublic.
Note: 
- Extracts data to leginfo_load_Thu
- Runs every Thursday at 10:00 PM
Sources:
- Leginfo
	- pubinfo_Thu.zip

file: opengov_load_Fri.sh
author: Daniel Mangin
date: 6/11/2015
Description: Grabs the pubinfo_Fri.zip and adds the daily files to capublic.
Note: 
- Extracts data to leginfo_load_Fri
- Runs every Friday at 10:00 PM
Sources:
- Leginfo
	- pubinfo_Fri.zip

file: opengov_load_Sat.sh
author: Daniel Mangin
date: 6/11/2015
Description: Grabs the pubinfo_Sat.zip and adds the daily files to capublic.
Note: 
- Extracts data to leginfo_load_Sat
- Runs every Saturday at 10:00 PM
Sources:
- Leginfo
	- pubinfo_Sat.zip


Python_Scripts/

===========

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

'''
File: Author_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Inserts the authors from capublic.bill_version_authors_tbl into the DDDB2015Apr.authors or DDDB2015Apr.committeeAuthors
- This script runs under the update script
- Fills table:
	authors (pid, bid, vid, contribution)
	CommitteeAuthors (cid, bid, vid)

Sources:
- Leginfo (capublic)
	- Pubinfo_2015.zip
	- Pubinfo_Mon.zip
	- Pubinfo_Tue.zip
	- Pubinfo_Wed.zip
	- Pubinfo_Thu.zip
	- Pubinfo_Fri.zip
	- Pubinfo_Sat.zip

- capublic
	- bill_version_author_tbl
'''

'''
File: Bill_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Inserts the authors from capublic.bill_tbl into DDDB2015Apr.Bill and capublic.bill_version_tbl into DDDB2015Apr.BillVersion
- This script runs under the update script
- Fills table:
	Bill (bid, type, number, state, status, house, session)
	BillVersion (vid, bid, date, state, subject, appropriation, substantive_changes)

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
	- bill_tbl
	- bill_version_tbl
'''

'''
File: billparse.py
Author: ???
Date: 6/11/2015

Description:
- Takes the bill_xml column from the capublic.bill_version_tbl and inserts it into the appropriate columns in DDDB2015Apr.BillVersion
- This script runs under the update script
- Fills table:
	BillVersion (title, digest, text)

Sources:
- Leginfo (capublic)
	- Pubinfo_2015.zip
	- Pubinfo_Mon.zip
	- Pubinfo_Tue.zip
	- Pubinfo_Wed.zip
	- Pubinfo_Thu.zip
	- Pubinfo_Fri.zip
	- Pubinfo_Sat.zip

- capublic
	- bill_version_tbl
'''

'''
File: Cal-Access-Accessor.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Goes through the file CVR_REGISTRATION_CD.TSV and places the data into DDDB2015Apr
- This script runs under the update script
- Fills table:
	LobbyingFirm (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
	Lobbyist (pid, filer_id)
	Person (last, first)
	LobbyistEmployer (filer_naml, filer_id, coalition)
	LobbyistEmployment (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
	LobbyistDirectEmployment (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
	LobbyingContracts (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr)

Sources:
- db_web_export.zip (California Access)
	- CVR_REGISTRATION_CD.TSV
'''

'''
File: Committee_CSV_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Goes through the file Committee_list.csv and places the data into DDDB2015Apr
- This script is used to get type of Committee
- Fills table:
	Committee(Type)

Sources:
- Committee_list.csv

'''

'''
DEPRECIATED SCRIPT. We use Get_Committees_Web.py instead
File: Committee_CSV_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers JSON data from OpenState and fills DDDB2015Apr.servesOn
-Used for daily update DDDB2015Apr
- Fills table:
	servesOn (pid, year, district, house, cid)

Sources
- OpenState

'''

'''
DEPRECIATED SCRIPT. We use Get_Committees_Web.py instead
File: Committee_CSV_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers JSON data from OpenState and fills DDDB2015Apr.Committee
- used for daily update of DDDB2015Apr
- Fills table:
	Committee (cid, house, name)

Sources
- OpenState

'''

'''
File: Get_Committees_Web.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Scrapes the Assembly and Senate Websites to gather current Committees and Membership and place them into
	DDDB2015Apr.Committee and DDDB2015Apr.servesOn
- Used for daily update Script
- Fills table:
	Committee (cid, house, name)
	servesOn (pid, year, district, house, cid)

Sources
- California Assembly Website
- California Senate Website

'''

'''
File: Get_Districts.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers JSON data from OpenState and fills DDDB2015Apr.District
- Used in the daily update script
- Fills table:
	District (state, house, did, note, year, geodata, region)

Sources:
- OpenState

'''

'''
DEPRECIATED SCRIPT, Use insert_Contributions_3_CSV.py
File: insert_Contributions.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers Contribution Data and puts it into DDDB2015.Contributions
- Used once for the Contributions.json
- Fills table:
	Contribution (pid, year, house, contributor, amount)

Source
- Contributions.json

'''

'''
File: insert_Contributions_CSV.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers Contribution Data and puts it into DDDB2015.Contributions
- Used once for the Insertion of all the Contributions
- Fills table:
	Contribution (id, pid, year, date, house, donorName, donorOrg, amount)

Sources:
- Maplight Data
	- cand_2001.csv
	- cand_2003.csv
	- cand_2005.csv
	- cand_2007.csv
	- cand_2009.csv
	- cand_2011.csv
	- cand_2013.csv
	- cand_2015.csv
'''

'''
File: insert_Gifts.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers Gift Data and puts it into DDDB2015.Gift
- Used once for the Insertion of all the Gifts
- Fills table:
	Gift (pid, schedule, sourceName, activity, city, cityState, value, giftDate, reimbursed, giftIncomeFlag, speechFlag, description)

Source:
- Gifts.txt

'''

'''
File: Leg_Bio_Extractor.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers Senate and Assembly Biographies and puts it into DDDB2015.Legislator.Bio
- Used once for the Insertion of all OfficialBios
- Fills table:
	Legislator (OfficialBio)

Sources:
- Senate_and_Assembly_Biographies.csv

'''

'''
File: legislator_migrate.py
Author: ???
Date: 6/11/2015

Description:
- Gathers Legislator Data from capublic.legislator_tbl and inserts the data into DDDB2015Apr.Person, DDDB2015Apr.Legislator, and DDDB2015Apr.Term
- Used in the daily update of DDDB2015Apr
- Fills table:
	Person (last, first)
	Legislator (pid)
	Term (pid, year, district, house, party)

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
	- legislator_tbl
'''

'''
File: Legislators.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gets a list of random Legislators and outputs them to a text file
- Only used for providing names for testing reasons
'''

'''
File: Lobbying_Firm_Name_Fix.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Used to correct names of Lobbying Firms gathered during the Lobbying Info
- Used as an import to the Cal-Access-Accessor.py to clean Lobbying Firm Names
'''

'''
File: Motion_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers the Motions from capublic.bill_motion_tbl and inserts the Motions into DDDB2015Apr.Motion
- Used in the daily update of DDDB2015Apr
- Fills table:
	Motion (mid, date, text)

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
	- bill_motion_tbl
'''

'''
File: Name_Fixes_Legislator_Migrate.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Used when Legislator names change in capublic to what we have in DDDB2015Apr
- Used in legislator_migrate.py to adjust the names if they are the same Person

'''

'''
File: Person_Name_Fix.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Cleans the all capitalized names in the Person table and revertes them to their proper titling
- Included in Cal-Access-Accessor.py to clean up Lobbyist Names

'''

'''
File: Test-Randomizer.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Used to grab 25% of random values form DDDB2015Apr
- Used for testing reasons on Drupal
'''

'''
File: Vote_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gets the Vote Data from capublic.bill_summary_vote into DDDB2015Apr.BillVoteSummary and capublic.bill_detail_vote into DDDB2015Apr.BillVoteDetail
- Used in daily update of DDDB2015Apr
- Fills Tables:
	BillVoteSummary (bid, mid, cid, VoteDate, ayes, naes, abstain, result)
	BillVoteDetail (pid, voteId, result)

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
	- bill_summary_vote_tbl
	- bill_detail_vote_tbl

'''








