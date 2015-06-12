@Echo off
cls
if '%1' NEQ '' Set pwd=%1
if '%pwd%' == '' Set /P pwd=Please enter your mysql root password:

cd \pubinfo
for /F %%t IN (tables_lc.lst) DO (
  if EXIST %%t.dat (
    echo Processing table: %%t
    mysql -hlocalhost -uroot -p%pwd% -Dcapublic -v -v -f < %%t.sql 2>&1 > %%t.log
  )
)

Set pwd=
@Echo done...
Pause
