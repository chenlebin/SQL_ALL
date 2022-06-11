--todo ORC矢量化查询
-- Hive的默认查询执行引擎一次处理一行，而矢量化查询执行是一种Hive针对ORC文件操作的特性，
-- 目的是按照每批1024行读取数据，并且一次性对整个记录整合（而不是对单条记录）应用操作，
-- 提升了像过滤, 联合, 聚合等等操作的性能。
-- 注意：要使用矢量化查询执行，就必须以ORC格式存储数据。

-- 开启矢量化查询
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

