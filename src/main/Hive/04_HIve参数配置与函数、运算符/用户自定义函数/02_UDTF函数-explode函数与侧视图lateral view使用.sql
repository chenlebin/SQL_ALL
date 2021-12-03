--练习：NBA总冠军球队名单分析
-- 使用explode函数
-- UDTF函数的限制：不能只查询一张表，但又想返回分别属于两张表（一张源表，一张虚拟表）的字段
--todo 语法限制的解决：
-- 1、采用多表连接
-- 2、使用侧视图lateral view
use db_df2;
create table if not exists NBA_team
(
    team_name string comment "球队名",
    year      int comment "获奖年份"
)
    row format delimited
        fields terminated by ",";

create table if not exists NBA_team_tmp
(
    team_name string comment "球队名",
    year      array<int> comment "获奖年份"
)
    row format delimited
        fields terminated by ","
        collection items terminated by "|";


load data local inpath "/root/hivedata/The_NBA_Championship.txt" into table NBA_team_tmp;

select *
from NBA_team_tmp;


select explode(year)
from NBA_team_tmp;

--多表查询竟然也不行离谱！！！
select tmp2.team_name, explode(tmp1.year)
from NBA_team_tmp as tmp1
         join NBA_team_tmp as tmp2 on tmp1.team_name = tmp2.team_name;


--todo 使用侧视图lateral view解决(类似join连接)
select a.team_name, b.year
from NBA_team_tmp a lateral view explode(a.year) b as year
order by b.year;
