#!/bin/sh
DB=$1
echo $DB
echo "Going to Main Directory!"
cd
echo "creating backup!"

today=$(date +%Y-%m-%d)
mysqldump -uroot --databases DDDB2015Apr > DDDB2015Apr-$today.sql
echo "back up created!"
cd
echo "running python scripts"
cd dd-Data3.0/Python_Scripts/UsedForUpdate
echo "Populating tables Person, Legislator, and Term..."
python legislator_migrate.py
python legislator_migrate.py
echo "Populating tables Lobbyist..."
python Cal-Access-Accessor.py
echo "Populating tables Bill and BillVersion..."
python Bill_Extract.py
echo "Adding Text and digest to Bills..."
python billparse.py
echo "Populating tables Author Table..."
python Author_Extract.py
echo "Populating tables Committee and servesOn Table..."
python Get_Committees_Web.py
echo "Populating Motion Table..."
python Motion_Extract.py
echo "Getting Votes..."
python Vote_Extract.py
echo "Getting Districts..."
python Get_Districts.py
echo "Getting Actions..."
python Action_Extract.py
echo "Finished!"
