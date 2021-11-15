//内部表删除时，删除元数据+HDFS中的实际数据
//外部表删除时，删除元数据，HDFS中的实际数据不删除

/*创建一个内部表（默认创建就是内部表）*/

------------------------------------


--默认情况下 创建的表就是内部表
create table db_df2.student
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
)
    row format delimited
        fields terminated by ',';

--可以使用DESCRIBE FORMATTED itheima.student
-- 来获取表的描述信息，从中可以看出表的类型。
describe formatted db_df2.student;

--创建外部表 需要关键字 external
--外部表数据存储路径不指定 默认规则和内部表一致
--也可以使用location关键字指定HDFS任意路径
create external table db_df2.student_ext
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
)
    row format delimited
        fields terminated by ','
    location '/stu';

--创建外部表 不指定location
create external table db_df2.student_ext_nolocation
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
)
    row format delimited
        fields terminated by ',';



describe formatted db_df2.student_ext;


describe formatted db_df2.student;


describe formatted db_df2.student_ext_nolocation;

use db_df2;
show tables;
/*查看映射是否成功*/
select *
from db_df2.student_ext;

select *
from db_df2.student;

-- 比较删除内部表和外部表的区别
drop table db_df2.student;
drop table db_df2.student_ext;
show tables;

--删除没有指定Location HDFS路径的外部表
drop table db_df2.student_ext_nolocation;

show tables;