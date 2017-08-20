#!/bin/sh
d=`date '+%Y%m%d'`
if [ ! -d data_$d ]; then
echo ""
echo "creating dir: \"data_$d\""
echo ""
fi
dir=data_$d
mkdir $dir
cd $dir
curl 'http://campaignfinance.cdn.sos.ca.gov/calaccess-documentation.zip' > doc_$d.zip
curl 'http://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip' > db_$d.zip
chown -R mysql data_$d
chmod -R o+r data_$d
cd ..
find $dir -type f -ls >> log_data_fetch.txt
echo "" >> log_data_fetch.txt
tail log_data_fetch.txt
exit