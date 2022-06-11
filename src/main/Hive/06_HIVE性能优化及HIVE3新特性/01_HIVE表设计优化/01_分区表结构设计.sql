-----------------------1、Hive表设计优化-----------------------------------
--分区表优化
--todo 分区表值得注意的是，分区字段partition column必须是表中没有的字段，因为分区字段是一个虚拟字段，
-- 可以通过给字段取一个别名，来给已经存在的字段作为分区字段
--切换数据库
use db_df2;


--创建表
create table tb_login
(
    userid    string,
    logindate string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/login.log' into table tb_login;

select *
from tb_login;

-- 统计3月24号的登录人数
select logintime
from tb_login
where logintime = '2021-03-24'
group by logintime;

--执行计划
explain extended
select logintime
from tb_login
where logintime = '2021-03-24'
group by logintime;

--创建分区表 按照登录日期分区
create table tb_login_part
(
    userid string
)
    partitioned by (logindate string)
    row format delimited fields terminated by '\t';

--开启动态分区 使用nonstrict非严格模式
set hive.exec.dynamic.partition.mode=nonstrict;

--按登录日期分区，需要分区时，不能使用load data，除非是特殊情况，例如插入的数据刚好是比表多一列，才会被Hive识别为动态分区插入
insert into table tb_login_part partition (logindate)
select *
from tb_login;

select *
from tb_login_part;

--基于分区表查询数据
explain extended
select logindate
from tb_login_part
where logindate = '2021-03-23'
   or logindate = '2021-03-24'
   or logindate = '2021-03-22'
group by logindate;
