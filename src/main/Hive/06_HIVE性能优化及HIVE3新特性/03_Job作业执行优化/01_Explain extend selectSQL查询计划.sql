--todo Explain查询计划
-- HiveQL是一种类SQL的语言，从编程语言规范来说是一种声明式语言，用户会根据查询需求提交声明式的HQL查询，
-- 而Hive会根据底层计算引擎将其转化成Mapreduce/Tez/Spark的job；
-- explain命令可以帮助用户了解一条HQL语句在底层的实现过程。通俗来说就是Hive打算如何去做这件事。
-- explain会解析HQL语句，将整个HQL语句的实现步骤、依赖关系、实现过程都会进行解析返回，
-- 可以了解一条HQL语句在底层是如何实现数据的查询及处理的过程，辅助用户对Hive进行优化。
-- 官网：https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Explain

--todo 语法： EXPLAIN [FORMATTED|EXTENDED|DEPENDENCY|AUTHORIZATION|] query
--FORMATTED：对执行计划进行格式化，返回JSON格式的执行计划
--EXTENDED：提供一些额外的信息，比如文件的路径信息
--DEPENDENCY：以JSON格式返回查询所依赖的表和分区的列表
--AUTHORIZATION：列出需要被授权的条目，包括输入与输出


--todo 每个查询计划由以下几个部分组成
--The Abstract Syntax Tree for the query
--抽象语法树（AST）：Hive使用Antlr解析生成器，可以自动地将HQL生成为抽象语法树

--The dependencies between the different stages of the plan
--Stage依赖关系：会列出运行查询划分的stage阶段以及之间的依赖关系

--The description of each of the stages
--Stage内容：包含了每个stage非常重要的信息，比如运行时的operator和sort orders等具体的信息

--例如：
use db_df2;
explain extended
select count(*) as cnt
from tb_emp01
where deptno = '10';






