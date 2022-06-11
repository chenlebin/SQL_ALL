--todo 数据倾斜
-- 现象
--1>分布式计算中最常见的，最容易遇到的问题就是数据倾斜；
--2>数据倾斜的现象是，当提交运行一个程序时，这个程序的大多数的Task都已经运行结束了，
--只有某一个Task一直在运行，迟迟不能结束，导致整体的进度卡在99%或者100%，
--这时候就可以判定程序出现了数据倾斜的问题。
--3>todo 数据倾斜的原因：数据分配不均衡

--todo 数据倾斜-Group by、Count(distinct)
--当程序中出现group by或者count（distinct）等分组聚合的场景时，如果数据本身是倾斜的，
--根据MapReduce的Hash分区规则，肯定会出现数据倾斜的现象。
--根本原因是因为分区规则导致的，所以可以通过以下三种方案来解决group by导致的数据倾斜的问题。

--todo 方案一：开启Map端聚合
--通过减少shuffle数据量和Reducer阶段的执行时间，避免每个Task数据差异过大导致数据倾斜
set hive.map.aggr=true;


--todo 方案二：实现随机分区
--distribute by用于指定底层按照哪个字段作为Key实现分区、分组等
--通过rand()函数随机值实现随机分区，避免数据倾斜
select *
from db_df2.dual distribute by rand();


--todo 方案三：数据倾斜时自动负载均衡   [已在hive-site.xml中配置写死 开启 ]
set hive.groupby.skewindata=true;
--开启该参数以后，当评估发现可能生发数据倾斜时，当前程序会自动通过两个MapReduce来运行
--第一个MapReduce自动进行随机分布到Reducer中，每个Reducer做部分聚合操作，输出结果
--第二个MapReduce将上一步聚合的结果再按照业务（group by key）进行处理，保证相同的分布到一起，最终聚合得到结果




