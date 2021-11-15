/*数据库相关的DDL
  1 create 创建
  2 desc 查看信息
  3 use 切换
  4 drop 删除
  5 alter 修改
     - set Dbproperties(属性名=属性值) 修改数据库的属性
     - set owner user 用户名 修改数据库所有者
     - set Location 文件路径 修改数据库位置
  */

show tables;
show databases;
use db_df2;
show materialized views;
show views;
-- todo 1：create database 创建数据库
create database if not exists first
    comment "第一个实验数据库"
    with DBPROPERTIES ("createdBy" = "陈乐斌");

show databases;

-- todo 2： describe database 查看数据库信息
-- extended 可以显示更多信息
desc database extended db_df2;


-- todo 3：use database 切换数据库
use db_df2;

use first;

create table xx
as
select *
from db_df2.wzry_pifu;

select *
from first.xx;


-- todo 4：drop database 删除数据库
-- 默认情况下只能删除空数据库，使用cascade删除数据库时可以直接删除带有表的数据库
drop database first cascade;


--todo 5：alter database 修改数据库
-- 修改之前先查看一下数据库的信息
desc database extended db_df2;

-- 更改数据库属性
alter database db_df2 set DBPROPERTIES ("createdBy" = "chenlebin");

-- 更改数据库所有者
alter database db_df2 set owner user root;

-- 更改数据库位置
alter database db_df2 set location "/user/hive/warehouse";

--查看修改之后的数据库信息：
desc database extended db_df2;












































