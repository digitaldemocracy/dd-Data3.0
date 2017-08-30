select bill_id, analysis_date
from bill_analysis_tbl
where bill_id like '%SCR1';


select count(distinct bill_id)
from bill_analysis_tbl;


