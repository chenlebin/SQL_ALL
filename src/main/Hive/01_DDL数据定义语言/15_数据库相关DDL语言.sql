/*数据库相关的DDL
  time ：2021-11-15-15:12
  1 create 创建
  2 desc 查看信息
  3 use 切换
  4 drop 删除
  5 alter 修改
     - set DBproperties(属性名=属性值) 修改数据库的属性
     - set owner user 用户名 修改数据库所有者
     - set Location 文件路径 修改数据库位置
  */

show tables;

show databases;

use db_df2;

show materialized views;

show views;

-- todo 1：create database 创建数据库
create database if not exists first_2
    comment "第一个实验数据库"
    --location "/nixx"
    with DBPROPERTIES ("createdBy" = "陈乐斌");

show databases;


-- todo 2： describe database 查看数据库信息
-- extended 可以显示更多信息
desc database extended first_2;

create table first_2.shabi
(
    name string,
    id   int,
    sex  string
) comment "临时表"
    row format delimited
        fields terminated by ",";

describe formatted first_2.shabi;

-- todo 3：use database 切换数据库
use db_df2;

use first_2;

create table first_2.xx
as
select *
from db_df2.wzry_pifu;

select *
from first_2.xx;

show tables from first_2;

-- todo 4：drop database 删除数据库
-- 默认情况下只能删除空数据库，使用cascade删除数据库时可以直接删除带有表的数据库
drop database first_2 cascade;
--todo cascade
--如果数据库中仍有表存在则报错：
-- Error while processing statement: FAILED: Execution Error, return code 1 from
-- org.apache.hadoop.hive.ql.exec.DDLTask. InvalidOperationException
-- (message:Database first_2 is not empty. One or more tables exist.)


--todo 5：alter database 修改数据库
-- 修改之前先查看一下数据库的信息
desc database extended db_df2;

-- 更改数据库属性
alter database db_df2 set DBPROPERTIES ("createdBy" = "chenlebin");

-- 更改数据库所有者
alter database db_df2 set owner user root;

-- 更改数据库位置
alter database first_2 set location "hdfs://mycluster/nixx";

--查看修改之后的数据库信息：
desc database extended first_2;

desc formatted db_df2.wzry_pifu;

show create table db_df2.wzry_pifu;

show tables from db_df2;

desc extended student_trans_agg;

show tables;

desc formatted student_trans_agg;





































































