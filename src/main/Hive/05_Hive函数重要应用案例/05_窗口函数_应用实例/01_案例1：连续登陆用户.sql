--基于以上的需求根据数据寻找规律，要想得到连续登陆用户，必须找到两个相同用户ID的行之间登陆日期之间的关系。
--例如：统计连续登陆两天的用户，只要用户ID相等，并且登陆日期之间相差1天即可。
--基于这个规律，我们有两种方案可以实现该需求。
---方案一：表中的数据自连接，构建笛卡尔积
---方案二：使用窗口函数来实现

SELECT *
FROM singer_wash;

DESC FORMATTED singer_wash;

--建表
use db_df2;

--数据样式：
--B	2021-03-24
drop table t_lianxu_user;

create table if not exists t_lianxu_user
(
    userid    STRING comment "登陆的用户id唯一标识",
    loginTime string comment "登陆时间"
)
    row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
--指定正则表达式
        WITH SERDEPROPERTIES (
        "input.regex" = "([A-Z])(\\s*)(\\d{4})([-])(\\d{2})([-])(\\d{2})"
        ) stored as textfile;

load data local inpath "/root/hivedata/login.log" into table db_df2.t_lianxu_user;

select *
from t_lianxu_user;



--1、连续登陆用户
--建表
create table tb_login
(
    userid    string,
    logintime string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/login.log' into table tb_login;

select *
from tb_login;

--自连接过滤实现
--a.构建笛卡尔积
select a.userid    as a_userid,
       a.logintime as a_logintime,
       b.userid    as b_userid,
       b.logintime as b_logintime
from tb_login a,
     tb_login b;

--上述查询结果保存为临时表
create table tb_login_tmp as
select a.userid    as a_userid,
       a.logintime as a_logintime,
       b.userid    as b_userid,
       b.logintime as b_logintime
from tb_login a,
     tb_login b;

--过滤数据：用户id相同并且登陆日期相差1
select a_userid,
       a_logintime,
       b_userid,
       b_logintime
from tb_login_tmp
where a_userid = b_userid
  and cast(substr(a_logintime, 9, 2) as int) - 1 = cast(substr(b_logintime, 9, 2) as int);

--统计连续两天登陆用户
select distinct a_userid
from tb_login_tmp
where a_userid = b_userid
  and cast(substr(a_logintime, 9, 2) as int) - 1 = cast(substr(b_logintime, 9, 2) as int);


----窗口函数实现
--连续登陆2天
select userid,
       logintime,
       --本次登陆日期的第二天
       date_add(logintime, 1)                                              as nextday,
       --按照用户id分区，按照登陆日期排序，取下一次登陆时间，取不到就为0
       lead(logintime, 1, 0) over (partition by userid order by logintime) as nextlogin
from tb_login;

--实现
with t1 as (
    select userid,
           logintime,
           --本次登陆日期的第二天
           date_add(logintime, 1)                                              as nextday,
           --按照用户id分区，按照登陆日期排序，取下一次登陆时间，取不到就为0
           lead(logintime, 1, 0) over (partition by userid order by logintime) as nextlogin
    from tb_login)
select distinct userid
from t1
where nextday = nextlogin;


--连续3天登陆
select userid,
       logintime,
       --本次登陆日期的第三天
       date_add(logintime, 2)                                              as nextday,
       --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
       lead(logintime, 2, 0) over (partition by userid order by logintime) as nextlogin
from tb_login;

select *
from singer_wash;


show create table singer_wash;

load data local inpath "/root/hivedata/21-11.txt" into table singer_wash;

--实现
with t1 as (
    select userid,
           logintime,
           --本次登陆日期的第三天
           date_add(logintime, 2)                                              as nextday,
           --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
           lead(logintime, 2, 0) over (partition by userid order by logintime) as nextlogin
    from tb_login)
select distinct userid
from t1
where nextday = nextlogin;

--连续N天
select userid,
       logintime,
       --本次登陆日期的第N天
       date_add(logintime, N - 1)                                              as nextday,
       --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
       lead(logintime, N - 1, 0) over (partition by userid order by logintime) as nextlogin
from tb_login;
