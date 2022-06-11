--todo 使用 unix_timestamp()函数
--指定格式日期转UNIX时间戳函数: unix_timestamp
select unix_timestamp('20111207 13:01:03', 'yyyyMMdd HH:mm:ss');
--todo 数据样式 这里我们故意将时间存为string类型图方便
--uid,date,state,county,cases,deaths
--1,2020/1/25,Illinois,Cook,1,0
create table if not exists t_usa_tmp
(
    uid    int comment "编号",
    u_date string comment "时间",
    state  string comment "州",
    county string comment "县",
    cases  int comment "确诊人数",
    deaths int comment "死亡人数"
)
    comment "疫情表，此处用于方便验证unix_timestam函数"
    row format delimited
        fields terminated by ',';

load data local inpath '/root/hivedata/xxxx-10.txt' overwrite into table t_usa_tmp;

select *
from t_usa_tmp;

explain extended
select u_date
from t_usa_tmp
where u_date = '2020/1/25'
  and county = 'Cook';
--from中的子查询一定要有给别名因为hive认为子查询是一个临时表
select t1.u_date
from (
         select u_date, county from t_usa_tmp where u_date = '2020/1/25' and county = 'Cook') t1
union all
select t2.u_date
from (
         select u_date, county
         from t_usa_tmp
         where county = 'Los Angeles') t2









