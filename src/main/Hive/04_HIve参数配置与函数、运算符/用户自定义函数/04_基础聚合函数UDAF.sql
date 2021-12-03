--------------基础聚合函数-------------------
--1、测试数据准备
use db_df2;
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
--验证
select *
from student;


--场景1：没有group by子句的聚合操作
--count(*)：所有行进行统计，包括NULL行
--count(1)：所有行进行统计，包括NULL行
--count(column)：对column中非Null进行统计
select count(*) as cnt1, count(1) as cnt2
from student;

select count(sex) as cnt3
from student;

--场景2：带有group by子句的聚合操作 注意group by语法限制
select sex, count(*) as cnt
from student
group by sex;

--场景3：select时多个聚合函数一起使用
select count(*) as cnt1, avg(age) as cnt2
from student;

--场景4：聚合函数和case when条件转换函数、coalesce函数、if函数使用
select sum(CASE WHEN sex = '男' THEN 1 ELSE 0 END)
from student;

select sum(if(sex = '男', 1, 0))
from student;

--场景5：聚合参数不支持嵌套聚合函数
select avg(count(*))
from student;

--场景6：聚合操作时针对null的处理
CREATE TABLE tmp_1
(
    val1 int,
    val2 int
);
INSERT INTO TABLE tmp_1
VALUES (1, 2),
       (null, 2),
       (2, 3);
select *
from tmp_1;
--第二行数据(NULL, 2) 在进行sum(val1 + val2)的时候会被忽略
select sum(val1), sum(val1 + val2)
from tmp_1;
--可以使用coalesce函数解决
select sum(coalesce(val1, 0)),
       sum(coalesce(val1, 0) + val2)
from tmp_1;

--场景7：配合distinct关键字去重聚合
--此场景下，会编译期间会自动设置只启动一个reduce task处理数据  可能造成数据拥堵
select count(distinct sex) as cnt1
from student;
--可以先去重 在聚合 通过子查询完成
--因为先执行distinct的时候 可以使用多个reducetask来跑数据
select count(*) as gender_uni_cnt
from (select distinct sex from student) a;

--案例需求：找出student中男女学生年龄最大的及其名字
--这里使用了struct来构造数据 然后针对struct应用max找出最大元素 然后取值
select sex,
       max(struct(age, name)).col1 as age,
       max(struct(age, name)).col2 as name
from student
group by sex;

select struct(age, name)
from student;
select struct(age, name).col1
from student;
select max(struct(age, name))
from student;