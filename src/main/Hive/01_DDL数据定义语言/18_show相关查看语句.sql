--1、显示所有数据库 SCHEMAS和DATABASES的用法 功能一样
show databases;

show schemas;

show functions;

desc function extended sum;

describe function extended explode;

--2、显示当前数据库所有表/视图/物化视图/分区/索引
show tables;

SHOW TABLES [IN database_name];
--指定某个数据库

--3、显示当前数据库下所有视图
Show Views;
SHOW VIEWS 'v_*'; -- hive中也有通配符，v_*代表,以v_开头的文件
SHOW VIEWS FROM db_df2; -- show views from database test1
SHOW VIEWS [IN/FROM database_name];

--4、显示当前数据库下所有物化视图
SHOW MATERIALIZED VIEWS from db_df2;

--5、显示表分区信息，分区按字母顺序列出，不是分区表执行该语句会报错
show partitions db_df2.usa_partition;
show partitions db_df2.student_trans;

--6、显示表/分区的扩展信息
SHOW TABLE EXTENDED [IN|FROM database_name] LIKE table_name;
show table extended like student;
describe formatted db_df2.student;

--7、显示表的属性信息
SHOW TBLPROPERTIES wzry_pifu;
show tblproperties student;

--8、显示表、视图的创建语句
SHOW CREATE TABLE db_df2.v_wzry_sheshou;
show create table student;

--9、显示表中的所有列，包括分区列。
SHOW COLUMNS (FROM | IN) table_name [(FROM | IN) db_name];
show columns in student_trans_agg;

show columns from db_df2.wzry_pifu;

desc database db_df2;

--10、显示当前支持的所有自定义和内置的函数
show functions;

--11、Describe desc
--查看表信息
desc extended db_df2.wzry_pifu;
--todo 查看表信息（格式化美观） ！！用的最多的！！
desc formatted db_df2.usa_partition;
--查看数据库相关信息
describe database db_df2;

--12、todo explain extended
-- 查看查询语句的执行计划（运行情况）
explain extended
select *
from student_trans;


use db_df2;

show databases;

show tables;

use db_df2;

show tables;

select *
from usa_zong_2022_02_10;

drop table c_ceshi_2022_02_09;


