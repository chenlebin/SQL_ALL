-----Conditional Functions 条件函数------------------
--使用之前课程创建好的student表数据
use db_df2;
select *
from student
limit 3;

describe function extended isnull;

--if条件判断: if(boolean testCondition, T valueTrue, T valueFalseOrNull)
select if(1 = 2, 100, 200);--200
select if(sex = '男', 'M', 'W')
from student
limit 3;
--可以利用if(,,)实现字段值替换

--空判断函数: isnull( a )
select isnull("allen");
select isnull(null);

--非空判断函数: isnotnull ( a )
select isnotnull("allen");
select isnotnull(null);

--空值转换函数: nvl(T value, T default_value)
select nvl("allen", "itcast");
select nvl(null, "itcast");

--非空查找函数: COALESCE(T v1, T v2, ...)
--返回参数中的第一个非空值；如果所有值都为NULL，那么返回NULL
select COALESCE(null, 11, 22, 33);
select COALESCE(null, null, null, 33);
select COALESCE(null, null, null);

--todo 条件转换函数: CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END
select case 100 when 50 then 'tom' when 100 then 'mary' else 'tim' end;
select case sex when '男' then 'male' else 'female' end
from student
limit 3;

--nullif( a, b ):
-- 如果a == b，则返回NULL，否则返回第一个
select nullif(11, 11);
select nullif(11, 12);

--assert_true(condition)
--如果'condition'不为真，则引发异常，否则返回null
SELECT assert_true(11 >= 0);
SELECT assert_true(-1 >= 0);--异常