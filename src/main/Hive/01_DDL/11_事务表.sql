use db_df2;

show tables;

/* 事务表注意事项：
 todo clustered by (id) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');
  必须为分桶表
  必须为orc文件格式
  表参数 transactional必须为true  TBLPROPERTIES('transactional'='true');
  不支持commit、begin、rollback等回滚操作，所有语言操作自动提交
  不支持外部表，不允许从非ACID会话读取/写入ACID表
  默认情况下事务配置为关闭，需配置参数开启使用

*/
---Hive事务表
--Step1：创建普通的表
drop table if exists db_df2.student;
create table student
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
)
    row format delimited
        fields terminated by ',';
--Step2：加载数据到普通表中
load data local inpath '/hivedata/students.txt' into table db_df2.student;
select *
from db_df2.student;

--Step3：执行更新操作
update student
set age = 66
where num = 95001;
--不是事务表执行失败


--2、创建Hive事务表
drop table if exists trans_student;
create table trans_student
(
    id   int,
    name String,
    age  int
) clustered by (id) into 2 buckets stored as orc TBLPROPERTIES ('transactional' = 'true');
--注意 事务表创建几个要素：开启参数、分桶表、存储格式orc、表属性

--3、针对事务表进行insert update delete操作
insert into trans_student
values (1, "allen", 18);

update trans_student
set age = 20
where id = 1;

delete
from trans_student
where id = 1;

select *
from trans_student;