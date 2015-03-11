#!/bin/sh
DB=$1
echo $DB
echo "remaking tables!"
mysql -uroot DDDB2015AprTest < DB-clear.sql
mysql -uroot DDDB2015AprTest < DB-create.sql
echo "Updating tables..."
echo "Repopulating capublic..."
cd
cd dd-Play/data/leginfo/
echo "Deleting capublic data..."
for j in capublic; \
do for i in `echo 'show tables ' |mysql -uroot $j \
|grep -v 'Tables_in'`; do mysql -uroot $j -e "truncate $i"; done; done
echo "Populating capublic..."
echo dmangin221 | sudo -S sh opengov_load.sh pubinfo_2015
echo dmangin221 | sudo python get_Bill_Version_xml_data.py
cd
echo "capublic repopulated!"
echo "Going to scripts!"
cd
cd dd-Data3.0/Python_Scripts/
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
echo "Finished!"