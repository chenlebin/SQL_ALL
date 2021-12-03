----Type Conversion Functions 类型转换函数-----------------
--todo 任意数据类型之间转换:cast
-- 如果能转换则转换，不能转换则为null
select cast(12.14 as bigint);

select cast(12.14 as string);

select cast("hello" as int);