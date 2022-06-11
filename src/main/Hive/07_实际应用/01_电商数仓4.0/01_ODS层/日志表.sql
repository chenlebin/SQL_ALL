--ODS层
--日志表
drop table if exists ods_log;
CREATE EXTERNAL TABLE ods_log
(
    `line` string
)
    PARTITIONED BY (`dt` string) -- 按照时间创建分区
    STORED AS -- 指定存储方式，读数据采用LzoTextInputFormat；
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '/warehouse/gmall/ods/ods_log' -- 指定数据在hdfs上的存储位置
;

------load语法规则----
LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
    [PARTITION (partcol1=val1, partcol2=val2...)]
-- hive3.0新特性指定输入类型和序列化/反序列化方法
LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
    [PARTITION (partcol1=val1, partcol2=val2...)]
    [INPUTFORMAT 'inputformat' SERDE 'serde'] (3.0 or later)

--将每日采集的日志数据装载到ODS层中
load data inpath "/origin_data/gmall/log/topic_log/2022-02-17" INTO TABLE ods_log
    partition (dt = '2021-10-13')
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' SERDE 'Lzo';

----创建Lzo索引文件到数仓中
--hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer
--/warehouse/gmall/ods/ods_log/dt=2021-10-13