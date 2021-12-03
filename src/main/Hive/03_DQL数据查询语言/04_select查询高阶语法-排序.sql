---------------Hive SQL select查询高阶语法------------------
---todo 1、order by
--根据字段进行全局排序
use db_df2;
select *
from t_usa_covid19_p
where count_date = "2021-01-28"
  and state = "California"--分区查询
order by deaths; --默认asc, nulls first 也可以手动指定nulls last

select *
from t_usa_covid19_p
where count_date = "2021-01-28"
  and state = "California"
order by deaths desc;
--指定desc nulls last

--todo 强烈建议将LIMIT与ORDER BY一起使用。避免数据集行数过大
--当hive.mapred.mode设置为strict严格模式时，使用不带LIMIT的ORDER BY时会引发异常。
select *
from t_usa_covid19_p
where count_date = "2021-01-28"
  and state = "California"
order by deaths desc
limit 3;

--todo 2、cluster by
--根据一个字段进行分组并排序
--cluster by 的分组规则是：HashFunc(字段)%ReduceTask个数和分桶规则和MapReduce的默认分组规则是一致的
select *
from student;
--不指定reduce task个数
--日志显示：Number of reduce tasks not specified. Estimated from input data size: 1
select *
from student cluster by num;

--手动设置reduce task个数
set mapreduce.job.reduces =2;
select *
from student cluster by num;


--todo 3、distribute by +sort by
--单独指定一个字段进行分组，一个字段进行排序
--案例：把学生表数据根据性别分为两个部分，每个分组内根据年龄的倒序排序。
--错误
select *
from student cluster by sex order by age desc;
select *
from student cluster by sex sort by age desc;

--正确
select *
from student distribute by sex sort by age desc;

--下面两个语句执行结果一样
select *
from student distribute by num sort by num;
select *
from student cluster by num;


