--todo 数据倾斜-Join
--1>Join操作时，如果两张表比较大，无法实现Map Join，只能走Reduce Join，
--那么当关联字段中某一种值过多的时候依旧会导致数据倾斜的问题；
--2>面对Join产生的数据倾斜，核心的思想是尽量避免Reduce Join的产生，优先使用Map Join来实现；
--3>但往往很多的Join场景不满足Map Join的需求，那么可以以下三种方案来解决Join产生的数据倾斜问题：

--todo 方案一：提前过滤，将大数据变成小数据，实现Map Join   谓词下推
select a.id, a.value1, b.value2
from table1 a
         join (select b.* from table2 b where b.ds >= '20181201' and b.ds < '20190101') c
              on (a.id = c.id)


--todo 方案二：使用Bucket Join
--如果使用方案一，过滤后的数据依旧是一张大表，那么最后的Join依旧是一个Reduce Join
--这种场景下，可以将两张表的数据构建为桶表，实现Bucket Map Join，避免数据倾斜


--todo 方案三：使用Skew Join     [已在hive-site.xml中配置写死 开启 ]
--Skew Join是Hive中一种专门为了避免数据倾斜而设计的特殊的Join过程
--这种Join的原理是将Map Join和Reduce Join进行合并，如果某个值出现了数据倾斜，
--就会将产生数据倾斜的数据单独使用Map Join来实现
--其他没有产生数据倾斜的数据由Reduce Join来实现，这样就避免了Reduce Join中产生数据倾斜的问题
--最终将Map Join的结果和Reduce Join的结果进行Union合并

-- 开启运行过程中skewjoin
set hive.optimize.skewjoin=true;
-- 如果这个key的出现的次数超过这个范围
set hive.skewjoin.key=100000;
-- 在编译时判断是否会产生数据倾斜
set hive.optimize.skewjoin.compiletime=true;
-- 不合并，提升性能
set hive.optimize.union.remove=true;
-- 如果Hive的底层走的是MapReduce，必须开启这个属性，才能实现不合并
set mapreduce.input.fileinputformat.input.dir.recursive=true;



