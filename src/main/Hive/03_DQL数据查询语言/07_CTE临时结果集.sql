-----------------Common Table Expressions（CTE）-----------------------------------
--select语句中的CTE(临时结果集)
use db_df2;
with q1 as (select num, name, age from student where num = 95002)
select *
from q1;

-- from风格
with q1 as (select num, name, age from student where num = 95002)
from q1
select *;

-- chaining CTEs 链式
with q1 as (select * from student where num = 95002),
     q2 as (select num, name, age from q1)
select *
from (select num from q2) a;


-- union
with q1 as (select * from student where num = 95002),
     q2 as (select * from student where num = 95004)
select *
from q1
union all
select *
from q2;

--视图，CTAS和插入语句中的CTE
-- insert
--创建一个和student一样表结构的表
create table s1 like student;

with q1 as (select * from student where num = 95002)
from q1
insert
overwrite
table
s1
select *;

select *
from s1;

-- ctas
create table s2 as
with q1 as (select * from student where num = 95002)
select *
from q1;

-- view
create view v1 as
with q1 as (select * from student where num = 95002)
select *
from q1;

select *
from v1;





