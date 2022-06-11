/*
todo （1）Map Join
todo （2）Reduce Join
todo （3）Bucket Join
*/

--todo （1）Map Join
-- 应用场景
--适合于小表join大表或者小表Join小表
-- 原理
--将小的那份数据给每个MapTask的内存都放一份完整的数据，大的数据每个部分都可以与小数据的完整数据进行join
--底层不需要经过shuffle，需要占用内存空间存放小的数据文件
-- 使用
--尽量使用Map Join来实现Join过程，Hive中默认自动开启了 todo Map Join：hive.auto.convert.join=true
--Hive中小表的大小限制
-- 2.0版本之前的控制属性
set hive.mapjoin.smalltable.filesize=25M;
-- 2.0版本开始由以下参数控制
set hive.auto.convert.join.noconditionaltask.size=512000000;


--todo （2）Reduce Join
-- 应用场景
--适合于大表Join大表
-- 原理
--将两张表的数据在shuffle阶段利用shuffle的分组来将数据按照关联字段进行合并
--必须经过shuffle，利用Shuffle过程中的分组来实现关联
--使用
--Hive会自动判断是否满足Map Join，如果不满足Map Join，则自动执行Reduce Join


--todo （3）Bucket Join  [已在hive-site.xml中配置写死 开启 ]
-- 应用场景
--适合于大表Join大表
-- 原理
--将两张表按照相同的规则将数据划分
--根据对应的规则的数据进行join
--减少了比较次数，提高了性能

--todo 1> 使用Bucket Join
--语法：clustered by colName
--参数：
set hive.optimize.bucketmapjoin = true;
--要求：分桶字段 = Join字段 ，桶的个数相等或者成倍数

--todo 2> 使用Sort Merge Bucket Join（SMB）
-- 基于有序的数据Join
--语法：clustered by colName sorted by (colName)
--参数
set hive.optimize.bucketmapjoin = true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.sortmerge.join.noconditionaltask=true;
--要求：分桶字段 = Join字段 = 排序字段 ，桶的个数相等或者成倍数



