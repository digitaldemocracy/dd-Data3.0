#!/bin/bash
# Simple script that dumps DDDB2015Dec and builds a local copy
#USAGE:
# 	copy_db <db_name> <local_mysql_user>
#Note: You need to type the password for your local db when prompted
DB_NAME=$1
LOCAL_USER=$2
#LOCAL_PWD=$3
REMOTE_HOST=digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com
REMOTE_USER=observer
REMOTE_PWD=abc123
REMOTE_DB=DDDB2015Dec 
echo 'Performing mysql dump of $(DB_NAME)'
mysqldump --single-transaction -h $REMOTE_HOST -P 3306 -u $REMOTE_USER -p$REMOTE_PWD $REMOTE_DB > dump.sql
echo 'Creating '$DB_NAME
mysqladmin -u $LOCAL_USER create $DB_NAME;
echo 'Loading dump.sql into '$DB_NAME
mysql -u $LOCAL_USER $DB_NAME < dump.sql
rm dump.sql
