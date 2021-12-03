---------------Union联合查询----------------------------
-- 将两张表竖着拼接起来 distinct 去重（默认就是distinct）
use db_df2;
--语法规则
select_statement UNION [ALL | DISTINCT] select_statement UNION [ALL | DISTINCT] select_statement ...;

--使用DISTINCT关键字与使用UNION默认值效果一样，都会删除重复行。
select num, name
from student_local
UNION
select num, name
from student_hdfs;
--和上面一样
select num, name
from student_local
UNION
DISTINCT
select num, name
from student_hdfs;

--使用ALL关键字会保留重复行。
select num, name
from student_local
UNION ALL
select num, name
from student_hdfs
limit 2;

--如果要将ORDER BY，SORT BY，CLUSTER BY，DISTRIBUTE BY或LIMIT应用于单个SELECT
--请将子句放在括住SELECT的括号内
SELECT num, name
FROM (select num, name from student_local LIMIT 2) subq1
UNION
SELECT num, name
FROM (select num, name from student_hdfs LIMIT 3) subq2;

--如果要将ORDER BY，SORT BY，CLUSTER BY，DISTRIBUTE BY或LIMIT子句应用于整个UNION结果
--请将ORDER BY，SORT BY，CLUSTER BY，DISTRIBUTE BY或LIMIT放在最后一个之后。
select num, name
from student_local
UNION
select num, name
from student_hdfs
order by num desc
limit 5;
