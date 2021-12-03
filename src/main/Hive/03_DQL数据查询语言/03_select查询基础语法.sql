---------------Hive SQL select查询基础语法------------------
/*
todo 执行顺序
from->where->group>having>order>select>limit


*/
use db_df2;
--todo 1、select_expr
--查询所有字段或者指定字段
select *
from t_usa_covid19_p;
select county, cases, deaths
from t_usa_covid19_p;
--查询匹配正则表达式的所有字段
SET hive.support.quoted.identifiers = none; --反引号不在解释为其他含义，被解释为正则表达式
select `^c.*`
from t_usa_covid19_p;
--查询当前数据库
select current_database();
--省去from关键字
--查询使用函数
select count(county)
from t_usa_covid19_p;


--todo 2、ALL DISTINCT
--去重时也是执行MapReduce程序所以执行的很慢
--返回所有匹配的行
select state
from t_usa_covid19_p;
--相当于
select all state
from t_usa_covid19_p;
--返回所有匹配的行 去除重复的结果
select distinct state
from t_usa_covid19_p;
--多个字段distinct 整体去重
select county, state
from t_usa_covid19_p;
select distinct county, state
from t_usa_covid19_p;
select distinct sex
from student;

--todo 3、WHERE CAUSE
--条件查询
select *
from t_usa_covid19_p
where 1 > 2; -- 1 > 2 返回false
select *
from t_usa_covid19_p
where 1 = 1;
-- 1 = 1 返回true
--where条件中使用函数 找出州名字母长度超过10位的有哪些
select *
from t_usa_covid19_p
where length(state) > 10;
--where子句支持子查询
SELECT *
FROM A
WHERE A.a IN (SELECT foo FROM B);

--todo 注意：where条件中不能使用聚合函数
--报错 SemanticException:Not yet supported place for UDAF 'count'
--聚合函数要使用它的前提是结果集已经确定。
--而where子句还处于“确定”结果集的过程中，因而不能使用聚合函数。
select state, count(deaths)
from t_usa_covid19_p
where count(deaths) > 100
group by state;

--可以使用Having实现
select state, count(deaths)
from t_usa_covid19_p
group by state
having count(deaths) > 100;


--todo 4、分区查询、分区裁剪
--找出来自加州，累计死亡人数大于1000的县 state字段就是分区字段 进行分区裁剪 避免全表扫描
select *
from t_usa_covid19_p
where state = "California"
  and deaths > 1000;
--多分区裁剪
select *
from t_usa_covid19_p
where count_date = "2021-01-28"
  and state = "California"
  and deaths > 1000;


--todo 5、GROUP BY
--分组
--根据state州进行分组
--SemanticException:Expression not in GROUP BY key 'deaths'
--deaths不是分组字段 报错
--todo 使用group by select_expr要么是分组字段要么是聚合函数中的字段
--state是分组字段 可以直接出现在select_expr中
select state, deaths
from t_usa_covid19_p
where count_date = "2021-01-28"
group by state;
--todo 错误

--被聚合函数应用
select state, sum(deaths)
from t_usa_covid19_p
where count_date = "2021-01-28"
group by state;



--todo 6、having
--having主要用于弥补where中不能使用聚合函数的缺陷
--统计死亡病例数大于10000的州
--where语句中不能使用聚合函数 语法报错
select state, sum(deaths)
from t_usa_covid19_p
where count_date = "2021-01-28"
  and sum(deaths) > 10000
group by state;

--先where分组前过滤（此处是分区裁剪），再进行group by分组， 分组后每个分组结果集确定 再使用having过滤
select state, sum(deaths)
from t_usa_covid19_p
where count_date = "2021-01-28"
group by state
having sum(deaths) > 10000;

--这样写更好 即在group by的时候聚合函数已经作用得出结果 having直接引用结果过滤 不需要再单独计算一次了
select state, sum(deaths) as cnts
from t_usa_covid19_p
where count_date = "2021-01-28"
group by state
having cnts > 10000;



--todo 7、limit
--没有限制返回2021.1.28 加州的所有记录
select *
from t_usa_covid19_p
where count_date = "2021-01-28"
  and state = "California";

--返回结果集的前5条
--todo 一个参数
select *
from t_usa_covid19_p
where count_date = "2021-01-28"
  and state = "California"
limit 5;

--返回结果集从第3行开始 共3行
--todo 两个参数
select *
from t_usa_covid19_p
where count_date = "2021-01-28"
  and state = "California"
limit 2,3; --注意 第一个参数偏移量是从0开始的0,1,2所以是从第三行到第五行
