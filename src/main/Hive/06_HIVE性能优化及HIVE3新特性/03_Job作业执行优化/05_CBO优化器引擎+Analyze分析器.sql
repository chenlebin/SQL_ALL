--todo CBO优化器引擎
-- 优化器引擎-背景
--Hive默认的优化器在解析一些聚合统计类的处理时，底层解析的方案有时候不是最佳的方案。
--例如当前有一张表【共1000条数据】，id构建了索引，id =100值有900条
-- 需求：查询所有id = 100的数据，SQL语句为：select * from table where id = 100;
--方案一
--由于id这一列构建了索引，索引默认的优化器引擎RBO，会选择先从索引中查询id = 100的值所在的位置，
--再根据索引记录位置去读取对应的数据，但是这并不是最佳的执行方案。
--方案二
--有id=100的值有900条，占了总数据的90%，这时候是没有必要检索索引以后再检索数据的，
--可以直接检索数据返回，这样的效率会更高，更节省资源，这种方式就是CBO优化器引擎会选择的方案。

-- todo RBO优化器和CBO优化器
--todo RBO
-- rule basic optimise：基于规则的优化器，根据设定好的规则来对程序进行优化
--todo CBO
-- cost basic optimise：基于代价的优化器，根据不同场景所需要付出的代价来合适选择优化的方案
--对数据的分布的信息【数值出现的次数，条数，分布】来综合判断用哪种处理的方案是最佳方案
--Hive中支持RBO与CBO这两种引擎，默认使用的是RBO优化器引擎。
--很明显CBO引擎更加智能，所以在使用Hive时，我们可以配置底层的优化器引擎为CBO引擎。
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;

--todo 优化器-Analyze分析器
--功能
--用于提前运行一个MapReduce程序将表或者分区的信息构建一些元数据【表的信息、分区信息、列的信息】，搭配CBO引擎一起使用
--语法

-- 构建分区信息元数据
ANALYZE TABLE tablename
    [PARTITION (partcol1[=val1], partcol2[=val2],...)]
    COMPUTE STATISTICS [noscan];

-- 构建列的元数据
ANALYZE TABLE tablename
    [PARTITION (partcol1[=val1], partcol2[=val2],...)]
    COMPUTE STATISTICS FOR COLUMNS ( columns name1, columns name2...) [noscan];

-- 查看元数据
DESC FORMATTED [tablename] [columnname];




