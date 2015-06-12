echo "Deleting capublic data..."
for j in capublic; \
do for i in `echo 'show tables ' |mysql -uroot $j \
|grep -v 'Tables_in'`; do mysql -uroot $j -e "truncate $i"; done; done