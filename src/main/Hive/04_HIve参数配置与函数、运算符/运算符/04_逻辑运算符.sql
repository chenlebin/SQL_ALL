---------------------Hive逻辑运算符-------------------
use db_df2;
--与操作: A AND B   如果A和B均为TRUE，则为TRUE，否则为FALSE。如果A或B为NULL，则为NULL。
select 1
from dual
where 3 > 1
  and 2 > 1;
--或操作: A OR B   如果A或B或两者均为FALSE，则为FALSE，否则为TRUE
select 1
from dual
where 0 > 1
   or 2 != 2;
--非操作: NOT A 、!A   如果A为FALSE，则为TRUE；如果A为NULL，则为NULL。否则为FALSE。
select 1
from dual
where not 2 > 1;
select 1
from dual
where !2 = 1;
--在:A IN (val1, val2, ...)  如果A等于任何值，则为TRUE。
select 1
from dual
where 11 in (11, 22, 33);
--不在:A NOT IN (val1, val2, ...) 如果A不等于任何值，则为TRUE
select 1
from dual
where 11 not in (22, 33, 44);
--逻辑是否存在: [NOT] EXISTS (subquery)
--将主查询的数据，放到子查询中做条件验证，根据验证结果（TRUE 或 FALSE）来决定主查询的数据结果是否得以保留。
select A.*
from A
where exists (select B.id from B where A.id = B.id);


--其他运算符
-- || 字符串拼接和concat的(A,B)的效果是一样的
select 'it' || 'cast';

select concat();


--复杂数据类型的构造：map、array、struct
select `array`(11, 22, 33)
from dual;
--复杂数据类型的取值：A[n] 下标取值  M[key] 键值取value   S.x字段取值