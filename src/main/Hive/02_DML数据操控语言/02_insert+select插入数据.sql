--RDBMS中insert语句的使用
INSERT INTO table_name (field1, field2, ...fieldN)
VALUES (value1, value2, ...valueN);

---------hive中insert+values----------------
create table t_test_insert
(
    id   int,
    name string,
    age  int
);

desc formatted t_test_insert;

insert into table t_test_insert
values (1, "allen", 18),
       (2, "sb", 22);

explain extended
insert into table t_test_insert
values (1, "allen", 18),
       (2, "sb", 22);

select *
from t_test_insert;

----------hive中insert+select OVERWRITE|INTO-----------------
--语法规则
--覆盖重写
INSERT OVERWRITE TABLE tablename1 [PARTITION (partcol1=val1, partcol2=val2...) [IF NOT EXISTS]] select_statement1
FROM from_statement;
--插入数据
INSERT INTO TABLE tablename1 [PARTITION (partcol1=val1, partcol2=val2...)] select_statement1
FROM from_statement;


--step1:创建一张源表student
drop table if exists student;
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
--加载数据
load data local inpath '/root/hivedata/students.txt' into table student;

select *
from student;

--step2：创建一张目标表  只有两个字段
create table student_from_insert
(
    sno   int,
    sname string
);
--使用insert+select插入数据到新表中
insert into table student_from_insert
select num, name
from student;

select *
from student;

select *
from student_from_insert;