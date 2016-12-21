#!/bin/bash
# Usage: ./new_opengov_load.sh <zip name>

if [ "$#" -eq 1 ]; then
   DAY=$1
else
   DAY=`TZ=America/Los_Angeles date +%a`
fi

BASE_DIR="$( cd "$( dirname "$0" )" && pwd )"
WORKING_DIR=$BASE_DIR"/current"
BACKUP_DIR=$BASE_DIR"/leginfo_"$DAY
ZIP_NAME="pubinfo_"

# On Sunday, the capublic database is cleared and repopulated with
# the new weekly pubinfo file.
if [ "$DAY" == "Sun" ]; then
   echo "Deleting capublic data..."
   for t in `echo 'show tables;' | mysql -uroot capublic | grep -v Tables_in`; do
      mysql -uroot capublic -e "truncate $t"
   done
   ZIP_NAME=$ZIP_NAME"2017"
else
   ZIP_NAME=$ZIP_NAME"daily_"$DAY
fi

# Clear the working directory (must delete the entire directory because
# there are too many files to remove using just rm).
rm -rf $WORKING_DIR

# Make the directory again.
mkdir $WORKING_DIR 
cd $WORKING_DIR
wget http://downloads.leginfo.legislature.ca.gov/$ZIP_NAME.zip
unzip $ZIP_NAME.zip
rm $ZIP_NAME.zip
cd ..

# MySQL needs read/execute permission, but is not owner/group.
chmod -R 775 $WORKING_DIR
cd $WORKING_DIR
for SQL_FILE in ~/dd-Data3.0/updateScripts/opengov_load/*
do
   echo "Loading ${SQL_FILE%.*}..."
   mysql -uroot --local-infile=1 -Dcapublic -f -v < $SQL_FILE
done

echo "Backing up files..."
# Backup the current zip files.
rm -rf $BACKUP_DIR
# Copy the files using -R to avoid the "argument list too long" error.
cp -R $WORKING_DIR $BACKUP_DIR
