--1、多行转多列
use db_df2;
--case when 语法1 case 不指定列 when 指定条件
select id,
       case
           when id < 2 then 'a'
           when id = 2 then 'b'
           else 'c'
           end as caseName
from tb_url;

--case when 语法2 case 指定列 when 指定值
select id,
       case id
           when 1 then 'a'
           when 2 then 'b'
           else 'c'
           end as caseName
from tb_url;


--建表
create table row2col1
(
    col1 string,
    col2 string,
    col3 int
) row format delimited fields terminated by '\t';
--加载数据到表中
load data local inpath '/root/hivedata/r2c1.txt' into table row2col1;

select *
from row2col1;

--sql最终实现
select col1                                         as col1,
       max(case col2 when 'c' then col3 else 0 end) as c,
       max(case col2 when 'd' then col3 else 0 end) as d,
       max(case col2 when 'e' then col3 else 0 end) as e
from row2col1
group by col1;


