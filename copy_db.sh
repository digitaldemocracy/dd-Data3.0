#!/bin/bash
# Simple script that dumps DDDB2015Dec and builds a local copy
#USAGE:
# 	copy_db <name of db you want to create> <local_mysql_user> <local_pwd>[optional]
DB_NAME=$1
LOCAL_USER=$2
LOCAL_PWD=${3:-none}
#LOCAL_PWD=$3
REMOTE_HOST=digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com
REMOTE_USER=observer
REMOTE_PWD=abc123
#REMOTE_USER=awsDB
#REMOTE_PWD=digitaldemocracy789
REMOTE_DB=DDDB2015Dec 

TABLES=(
Action
Behests
BillSponsorRolls
BillTypes
Committee
CommitteeHearings
Contribution
DeprecatedOrganization
DeprecatedPerson
District
GeneralPublic
Gift
Hearing
HearingAgenda
House
LegAnalystOffice
LegAnalystOfficeRepresentation
LegAvgPercentParticipation
LegislativeStaff
LegislativeStaffRepresentation
Legislator
LegislatureOffice
LegOfficePersonnel
LegParticipation
LegStaffGifts
LobbyingContracts
LobbyingFirm
LobbyingFirmState
Lobbyist
LobbyistDirectEmployment
LobbyistEmployer
LobbyistEmployment
LobbyistRepresentation
Motion
OfficePersonnel
OrgAlignments
Organizations
Payors
Person
servesOn
SpeakerProfileTypes
State
StateAgency
StateAgencyRep
StateAgencyRepRepresentation
StateConstOffice
StateConstOfficeRep
StateConstOfficeRepRepresentation
Term
Video
FrequentDonors
)

TABLES_STRING=''
for TABLE in "${TABLES[@]}"
do :
   TABLES_STRING+=" ${TABLE}"
done

BILL_TABLES=(
authors
Bill
BillDiscussion
BillSponsors
BillVersion
BillVoteSummary
CommitteeAuthors
)

BILL_TABLES_STRING=''
for TABLE in "${BILL_TABLES[@]}"
do :
   BILL_TABLES_STRING+=" ${TABLE}"
done

CURRENT_UTTER="CREATE VIEW currentUtterance AS SELECT uid, vid, pid, time, endTime, text, type, alignment, state, did, lastTouched FROM Utterance WHERE current = TRUE AND finalized = TRUE ORDER BY time DESC"

BILL_ALIGN="CREATE VIEW BillAlignments AS SELECT MAX(u.uid) AS uid, l.pid, u.alignment, u.did FROM Lobbyist l JOIN currentUtterance u ON l.pid = u.pid WHERE u.did IS NOT NULL GROUP BY l.pid, u.alignment, u.did"

BILL_SUB="bid like '%20152016%'"
UTTER_SUB="current = 1 and finalized = 1"
BVD_SUB="voteId in (select voteId from BillVoteSummary where bid like '%20152016%')"


echo 'Performing mysql dump of '$REMOTE_DB
mysqldump -h $REMOTE_HOST -P 3306 -u $REMOTE_USER -p$REMOTE_PWD $REMOTE_DB --single-transaction $TABLES_STRING > dump.sql
mysqldump -h $REMOTE_HOST -P 3306 -u $REMOTE_USER -p$REMOTE_PWD --where "$BILL_SUB" $REMOTE_DB --single-transaction $BILL_TABLES_STRING > dumpBill.sql
mysqldump -h $REMOTE_HOST -P 3306 -u $REMOTE_USER -p$REMOTE_PWD --where "$UTTER_SUB" $REMOTE_DB --single-transaction Utterance > dumpUtter.sql
mysqldump -h $REMOTE_HOST -P 3306 -u $REMOTE_USER -p$REMOTE_PWD --where "$BVD_SUB" $REMOTE_DB --single-transaction BillVoteDetail > dumpBVD.sql

echo 'Creating '$DB_NAME
mysqladmin -u $LOCAL_USER create $DB_NAME;

if [ "$LOCAL_PWD" == "none" ]; then 
	echo 'Loading dump.sql into '$DB_NAME
	mysql -u $LOCAL_USER $DB_NAME < dumpBill.sql
	mysql -u $LOCAL_USER $DB_NAME < dump.sql
	mysql -u $LOCAL_USER $DB_NAME < dumpUtter.sql
	mysql -u $LOCAL_USER $DB_NAME --execute "$CURRENT_UTTER"
	mysql -u $LOCAL_USER $DB_NAME --execute "$BILL_ALIGN"
	mysql -u $LOCAL_USER $DB_NAME < dumpBVD.sql
else
	echo 'Loading dump.sql into '$DB_NAME
	mysql -u $LOCAL_USER -p$LOCAL_PWD $DB_NAME < dumpBill.sql
	mysql -u $LOCAL_USER -p$LOCAL_PWD $DB_NAME < dump.sql
	mysql -u $LOCAL_USER -p$LOCAL_PWD $DB_NAME < dumpUtter.sql
	mysql -u $LOCAL_USER -p$LOCAL_PWD $DB_NAME --execute "$CURRENT_UTTER"
	mysql -u $LOCAL_USER -p$LOCAL_PWD $DB_NAME --execute "$BILL_ALIGN"
	mysql -u $LOCAL_USER -p$LOCAL_PWD $DB_NAME < dumpBVD.sql
fi

rm dump.sql
rm dumpBill.sql
rm dumpUtter.sql
rm dumpBVD.sql
