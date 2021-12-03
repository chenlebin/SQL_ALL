--4、单列转多行
use db_df2;
select explode(split("a,b,c,d", ","));

--创建表
create table col2row2
(
    col1 string,
    col2 string,
    col3 string
) row format delimited fields terminated by '\t';

--加载数据
load data local inpath '/root/hivedata/c2r2.txt' into table col2row2;

select *
from col2row2;

select explode(split(col3, ','))
from col2row2;

--SQL最终实现 explode配合侧视图later view使用
select col1,
       col2,
       lv.col3 as col3
from col2row2
         lateral view
             explode(split(col3, ',')) lv as col3;



