select *
from db_df2.usa_zong;
use db_df2;

show tables;

select state, count(cases) as state_cnt
from usa_zong
group by state;

explain
select state, count(*) as state_cnt
from usa_zong
group by state;









































