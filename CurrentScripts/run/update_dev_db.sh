#!/bin/bash

echo "Starting Dev DB Update"

echo "Dump live DB"
mysqldump -h dddb-test.chzg5zpujwmo.us-west-2.rds.amazonaws.com -uawsDB -pdigitaldemocracy789 DDDB2016Aug > db_dump.sql
echo "Dump finished"

array=(DDDB2016Aug)
for i in "${array[@]}"
do
    echo "Updating database: " $i
    mysqladmin -uroot --force drop $i
    mysqladmin -uroot create $i
    mysql -uroot $i < db_dump.sql
    echo "Finished updating:" $i
done

rm db_dump.sql
