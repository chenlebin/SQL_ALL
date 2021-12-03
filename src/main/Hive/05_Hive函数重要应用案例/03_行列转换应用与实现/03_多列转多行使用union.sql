--3、多列转多行
use db_df2;
select 'b', 'a', 'c'
union
select 'a', 'b', 'c'
union
select 'a', 'b', 'c';


--创建表
create table col2row1
(
    col1 string,
    col2 int,
    col3 int,
    col4 int
) row format delimited fields terminated by '\t';

--加载数据
load data local inpath '/root/hivedata/c2r1.txt' into table col2row1;

select *
from col2row1;

--最终实现
select col1, 'c' as col2, col2 as col3
from col2row1
UNION ALL
select col1, 'd' as col2, col3 as col3
from col2row1
UNION ALL
select col1, 'e' as col2, col4 as col3
from col2row1;