----------todo 窗口函数语法树------------
-- todo 关键字：OVER
Function(arg1,..., argn) OVER ([PARTITION BY <...>] [ORDER BY <....>] [<window_expression>])

--其中Function(arg1,..., argn) 可以是下面分类中的任意一个
    --聚合函数：比如sum max avg等
    --排序函数：比如rank row_number dense_rank等
    --分析函数：比如lead lag first_value等

--OVER [PARTITION BY <...>] 类似于group by 用于指定分组  每个分组你可以把它叫做窗口
--如果没有PARTITION BY 那么整张表的所有行就是一组

--[ORDER BY <....>]  用于指定每个分组内的数据排序规则 支持ASC、DESC

--[<window_expression>] 窗口表达式 用于指定每个窗口中 操作的数据范围 默认是窗口中所有行


--建表加载数据
use db_df2;
drop table if exists employee;
CREATE TABLE employee
(
    id     int,
    name   string,
    deg    string,
    salary int,
    dept   string
) row format delimited
    fields terminated by ',';

load data local inpath '/root/hivedata/employee.txt' into table employee;

select *
from employee;

----sum+group by普通常规聚合操作------------
select dept, sum(salary) as total
from employee
group by dept;

----sum+窗口函数聚合操作------------
select id, name, deg, salary, dept, sum(salary) over (partition by dept) as total
from employee;
