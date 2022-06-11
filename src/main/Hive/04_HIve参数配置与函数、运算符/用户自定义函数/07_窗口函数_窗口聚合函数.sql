---------------------
---建表并且加载数据---
--用户浏览网页的次数表
create table website_pv_info
(
    cookieid   string,
    createtime string, --day
    pv         int
) row format delimited
    fields terminated by ',';


--用户浏览网页的url
create table website_url_info
(
    cookieid   string,
    createtime string, --访问时间
    url        string  --访问页面
) row format delimited
    fields terminated by ',';


load data local inpath '/root/hivedata/website_pv_info.txt' into table website_pv_info;
load data local inpath '/root/hivedata/website_url_info.txt' into table website_url_info;

select *
from website_pv_info;

select *
from website_url_info;


-----窗口聚合函数的使用-----------
--1、求出每个用户总pv数  sum+group by普通常规聚合操作
-- 常规聚合操作不可避免的需要使用group by进行分组，组内聚合，但是使用group by之后的查询字段是有限制的
-- todo group by 查询中，查询字段只能是分组字段或者聚合函数中的字段

select cookieid, sum(pv) as total_pv
from website_pv_info
group by cookieid
order by cookieid, total_pv;

select cookieid, createtime, sum(pv) over (partition by cookieid order by cookieid)
from website_pv_info;


--2、sum+窗口函数 窗口聚合函数 总共有四种用法 注意是整体聚合 还是累积聚合
--sum(...) over( )对表所有行整体求和
--sum(...) over( order by ... ) 对表所有行连续累积求和，并进行排序
--sum(...) over( partition by... ) 分组内所有行整体求和
--sum(...) over( partition by... order by ... ) 在每个分组内，连续累积求和，并进行排序

--需求：求出网站总的pv数 所有用户所有访问加起来
--sum(...) over( )对表所有行求和
select cookieid,
       createtime,
       pv,
       sum(pv) over () as total_pv --注意这里窗口函数是没有partition by 也就是没有分组  全表所有行
from website_pv_info;

--需求：求出每个用户总pv数
--sum(...) over( partition by... )，同组内所行求和
select cookieid,
       createtime,
       pv,
       sum(pv) over (partition by cookieid) as total_pv
from website_pv_info;

--需求：求出每个用户截止到当天，累积的总pv数
--sum(...) over( partition by... order by ... )，在每个分组内，连续累积求和
select cookieid,
       createtime,
       pv,
       sum(pv) over (partition by cookieid order by createtime) as current_total_pv
from website_pv_info;


