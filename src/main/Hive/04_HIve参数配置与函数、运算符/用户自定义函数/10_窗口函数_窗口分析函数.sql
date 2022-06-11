-----------窗口分析函数----------
-- LAG(col,n,DEFAULT) 用于统计窗口内往上第n行值
--     第一个参数为列名，第二个参数为往上第n行（可选，默认为1），第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）；
-- LEAD(col,n,DEFAULT) 用于统计窗口内往下第n行值
--     第一个参数为列名，第二个参数为往下第n行（可选，默认为1），第三个参数为默认值（当往下第n行为NULL时候，取默认值，如不指定，则为NULL）；
-- FIRST_VALUE 取分组内排序后，截止到当前行，第一个值
-- LAST_VALUE 取分组内排序后，截止到当前行，最后一个值

use db_df2;

--LAG
SELECT cookieid,
       createtime,
       url,
       ROW_NUMBER() OVER (PARTITION BY cookieid ORDER BY createtime)                              AS rn,
       LAG(createtime, 1, '1970-01-01 00:00:00') OVER (PARTITION BY cookieid ORDER BY createtime) AS last_1_time,
       LAG(createtime, 2) OVER (PARTITION BY cookieid ORDER BY createtime)                        AS last_2_time
FROM website_url_info;


--LEAD
SELECT cookieid,
       createtime,
       url,
       ROW_NUMBER() OVER (PARTITION BY cookieid ORDER BY createtime)                               AS rn,
       LEAD(createtime, 1, '1970-01-01 00:00:00') OVER (PARTITION BY cookieid ORDER BY createtime) AS next_1_time,
       LEAD(createtime, 2) OVER (PARTITION BY cookieid ORDER BY createtime)                        AS next_2_time
FROM website_url_info;

--FIRST_VALUE
SELECT cookieid,
       createtime,
       url,
       ROW_NUMBER() OVER (PARTITION BY cookieid ORDER BY createtime)     AS rn,
       FIRST_VALUE(url) OVER (PARTITION BY cookieid ORDER BY createtime) AS first1
FROM website_url_info;

--LAST_VALUE
SELECT cookieid,
       createtime,
       url,
       ROW_NUMBER() OVER (PARTITION BY cookieid ORDER BY createtime)    AS rn,
       LAST_VALUE(url) OVER (PARTITION BY cookieid ORDER BY createtime) AS last1
FROM website_url_info;






