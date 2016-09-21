mysqldump -h digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com -P 3306 -u awsDB -pdigitaldemocracy789 DDDB2015Dec > dump.sql

    mysqladmin -h digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com -P 3306 -u awsDB -pdigitaldemocracy789 create AndrewTest;

mysql -h digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com -P 3306 -u awsDB -pdigitaldemocracy789 AndrewTest2 < dump.sql

mysql -h dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com -P 3306 -u awsDB -pdigitaldemocracy789 DDDB2016Aug < dump.sql

