/*使用location字段指定HDFS文件系统上的任意一个结构化文件就可以
  以此为映射文件建表*/
/*数据样式*/
//date,county,state,cases,deaths
//2020/1/21,Snohomish,Washington,1,0

use db_df2;
create table usa_zong
(
    usa_date date,--<yyyy-MM-dd>,
    county   string,
    state    string,
    cases    int,
    deaths   int
) comment "疫情数据"
    row format delimited
        fields terminated by ","
    location "/MapReduce/usa_scout/input";

select *
from db_df2.usa_zong;





