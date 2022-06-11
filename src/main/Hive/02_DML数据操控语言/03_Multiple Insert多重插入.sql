------------multiple inserts----------------------
use db_df2;
--当前库下已有一张表student
select *
from student;
--创建两张新表
create table student_insert1
(
    sno int
);
create table student_insert2
(
    sname string
);

--正常思路来讲
--一张源表扫描了两次
insert into student_insert1
select num
from student;

insert into student_insert2
select name
from student;


--多重插入  一次扫描 多次插入 todo from风格
from student
insert
overwrite
table
student_insert1
select num
insert
overwrite
table
student_insert2
select name;

explain extended
from student
insert
overwrite
table
student_insert1
select num
insert
overwrite
table
student_insert2
select name;

select *
from student_insert1;

select *
from student_insert2;