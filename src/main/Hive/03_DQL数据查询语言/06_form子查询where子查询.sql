------------子查询Subqueries--------------
use db_df2;
--from子句中子查询（Subqueries）
--todo 必须给子查询起一个别名，因为from子句中的每个表都必须有一个名称
--子查询
SELECT num
FROM (
         select num, name
         from student_local
     ) tmp;

--包含UNION ALL的子查询的示例
SELECT t3.name
FROM (
         select num, name
         from student_local
         UNION
         distinct
         select num, name
         from student_hdfs
     ) t3;


--where子句中子查询（Subqueries）
--不相关子查询，相当于IN、NOT IN,子查询只能选择一个列。
--（1）执行子查询，其结果不被显示，而是传递给外部查询，作为外部查询的条件使用。
--（2）执行外部查询，并显示整个结果。　　
SELECT *
FROM student_hdfs
WHERE student_hdfs.num IN (select num from student_local limit 2);

--相关子查询，指EXISTS和NOT EXISTS子查询
--子查询的WHERE子句中支持对父查询的引用
SELECT A
FROM T1
WHERE EXISTS (SELECT B FROM T2 WHERE T1.X = T2.Y);