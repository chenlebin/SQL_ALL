-------------------------增强聚合-------------------
--一般用于多维分析
use db_df2;
--表创建并且加载数据
CREATE TABLE cookie_info
(
    month    STRING,
    day      STRING,
    cookieid STRING
) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

load data local inpath '/root/hivedata/cookie_info.txt' into table cookie_info;

select *
from cookie_info;

--todo grouping sets---------
-- grouping sets 是将多个group by逻辑写在一个sql语句中的便利写法，
-- grouping_id表示这一组结果属于哪个分组集合，
-- 等价于将不同维度的group by结果集进行union all
-- 例如grouping sets有abc三个维度进行则组合的情况有：
-- (a),(b),(c)
SELECT month,
       day,
       COUNT(DISTINCT cookieid) AS nums,
       GROUPING__ID
FROM cookie_info
GROUP BY month, day
    GROUPING SETS ( month, day) --这里是关键
ORDER BY GROUPING__ID;

--grouping_id表示这一组结果属于哪个分组集合，
--根据grouping sets中的分组条件month，day，1是代表month，2是代表day

--等价于
SELECT month, NULL, COUNT(DISTINCT cookieid) AS nums, 1 AS GROUPING__ID
FROM cookie_info
GROUP BY month
UNION ALL
SELECT NULL as month, day, COUNT(DISTINCT cookieid) AS nums, 2 AS GROUPING__ID
FROM cookie_info
GROUP BY day;

--再比如
SELECT month,
       day,
       COUNT(DISTINCT cookieid) AS nums,
       GROUPING__ID
FROM cookie_info
GROUP BY month, day
    GROUPING SETS ( month, day, ( month, day)) --1 month   2 day    3 (month,day) 三个分组的查询结果集union all
ORDER BY GROUPING__ID;

--等价于
SELECT month, NULL, COUNT(DISTINCT cookieid) AS nums, 1 AS GROUPING__ID
FROM cookie_info
GROUP BY month
UNION ALL
SELECT NULL, day, COUNT(DISTINCT cookieid) AS nums, 2 AS GROUPING__ID
FROM cookie_info
GROUP BY day
UNION ALL
SELECT month, day, COUNT(DISTINCT cookieid) AS nums, 3 AS GROUPING__ID
FROM cookie_info
GROUP BY month, day;

--todo cube---------------
-- cube表示根据group by的维度的所有的组合进行聚合 Union all
-- 对于cube来说如果有几个维度，则所有组合的总个数为2^n
-- 例如cube有abc三个维度进行则组合的情况有：
-- (a,b,c),(a,b),(a,c),(b,c),(a),(b),(c),()

SELECT month,
       day,
       COUNT(DISTINCT cookieid) AS nums,
       GROUPING__ID
FROM cookie_info
GROUP BY month, day
WITH CUBE
ORDER BY GROUPING__ID;

--等价于
SELECT NULL, NULL, COUNT(DISTINCT cookieid) AS nums, 0 AS GROUPING__ID
FROM cookie_info
UNION ALL
SELECT month, NULL, COUNT(DISTINCT cookieid) AS nums, 1 AS GROUPING__ID
FROM cookie_info
GROUP BY month
UNION ALL
SELECT NULL, day, COUNT(DISTINCT cookieid) AS nums, 2 AS GROUPING__ID
FROM cookie_info
GROUP BY day
UNION ALL
SELECT month, day, COUNT(DISTINCT cookieid) AS nums, 3 AS GROUPING__ID
FROM cookie_info
GROUP BY month, day;

--todo rollup-------------
-- rollup是cube的子集，以左侧的维度为主，从该维度进行层级聚合：
-- 例如rollup有abc三个维度进行则组合的情况有：
-- (a,b,c),(a,b),(a),()

--比如，以month维度进行层级聚合：
SELECT month,
       day,
       COUNT(DISTINCT cookieid) AS nums,
       GROUPING__ID
FROM cookie_info
GROUP BY month, day
WITH ROLLUP
ORDER BY GROUPING__ID;

--把month和day调换顺序，则以day维度进行层级聚合：
SELECT day,
       month,
       COUNT(DISTINCT cookieid) AS uv,
       GROUPING__ID
FROM cookie_info
GROUP BY day, month
WITH ROLLUP
ORDER BY GROUPING__ID;



-------------------------------------------------------------------
--验证测试
--count(*)：所有行进行统计，包括NULL行
--count(1)：所有行进行统计，包括NULL行
--count(column)：对column中非Null进行统计
select *
from t_all_hero_part_dynamic
where role = "archer";
select count(*), count(1), count(role_assist)
from t_all_hero_part_dynamic
where role = "archer";



