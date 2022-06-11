/*
   todo       分桶表的数据加载
time：2021-11-4-14:03
分通表的数据加载和分区表的动态数据加载类似
都需要有一个原表，然后执行insert+select
将原表的数据进行查询插入操作之后导入到分桶/分区表中
*/
-- 分桶表建表语句
-- CREATE [EXTERNAL] TABLE [db_name.]table_name
-- [(col_name data_type, ...)]
-- CLUSTERED BY (col_name)
-- INTO N BUCKETS;


CREATE TABLE db_df2.t_usa_covid19_bucket
(
    count_date string,
    county     string,
    state      string,
    fips       int,
    cases      int,
    deaths     int
)
    CLUSTERED BY (state) INTO 5 BUCKETS;
--分桶的字段一定要是表中已经存在的字段


--根据state州分为5桶 todo 每个桶内根据cases确诊病例数倒序排序
CREATE TABLE db_df2.t_usa_covid19_bucket_sort
(
    count_date string,
    county     string,
    state      string,
    fips       int,
    cases      int,
    deaths     int
)
    CLUSTERED BY (state)
        sorted by (cases desc) INTO 5 BUCKETS;
--指定每个分桶内部根据 cases倒序排序


--step1:开启分桶的功能 从Hive2.0开始不再需要设置
set hive.enforce.bucketing=true;

--step2:把源数据加载到普通hive表中
drop table if exists t_usa_covid19;
CREATE TABLE db_df2.t_usa_covid19
(
    count_date string,
    county     string,
    state      string,
    fips       int,
    cases      int,
    deaths     int
)
    row format delimited fields terminated by ",";

select *
from t_usa_covid19;


--step3:使用insert+select语法将数据加载到分桶表中
insert into t_usa_covid19_bucket
select *
from t_usa_covid19;

--step3:使用insert+select语法将数据加载到分桶排序表中
insert into t_usa_covid19_bucket_sort
select *
from t_usa_covid19;

select *
from t_usa_covid19_bucket;

--基于分桶字段state查询来自于New York州的数据
--不再需要进行全表扫描过滤
--根据分桶的规则hash_function(New York) mod 5计算出分桶编号
--查询指定分桶里面的数据 就可以找出结果  此时是分桶扫描而不是全表扫描
explain extended
select *
from t_usa_covid19_bucket
where state = "New York";


select *
from t_usa_covid19_bucket_sort;



























