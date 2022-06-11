--如果要实现以上需求，首先要统计出每个用户每个月的消费总金额，分组实现聚合，但是需要按照用户ID，
--将该用户这个月之前的所有月份的消费总金额进行累加实现。
--该需求可以通过两种方案来实现：
--方案一：分组统计每个用户每个月的消费金额，然后构建自连接，根据条件分组聚合
--方案二：分组统计每个用户每个月的消费金额，然后使用窗口聚合函数实现
create table if not exists t_anli2_xiaofei
(
    userid string comment "用户ID",
    mth    string comment "月份",
    monet  int comment "单笔消费"
)
    row format delimited
        fields terminated by '\t';

load data local inpath "/root/hivedata/money.tsv" into table t_anli2_xiaofei;

select *
from t_anli2_xiaofei;
--创建一个临时表

create table t_anli2_xiaofei_tmp
as
select userid,
       mth,
       sum(monet) m_money
from t_anli2_xiaofei
group by userid, mth;

select *
from t_anli2_xiaofei_tmp;

--A	2021-01	5
select userid,
       mth,
       m_money,
       sum(m_money) over (partition by userid order by mth)
from t_anli2_xiaofei_tmp;




















