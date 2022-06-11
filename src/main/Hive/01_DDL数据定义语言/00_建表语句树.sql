--------------------------------建表语句树-----------------------------------
--todo 注意 建表语句不一定需要写这么全，但是顺序绝对得按这个语句树来，否则会建表失败
CREATE
[TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name--表名
[(col_name data_type [COMMENT col_comment], ... ]--字段名 字段注释
[COMMENT table_comment]--表注释
[PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]--分区 分区注释
[CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]--分桶 分桶内排序规则
[ROW FORMAT DELIMITED|SERDE serde_name WITH SERDEPROPERTIES (property_name=property_value,...)]--分隔符/更换序列化方法
[STORED AS file_format]--设置文件存储格式 orc
[LOCATION hdfs_path]--设置文件路径，默认为/user/hive/warehouse/库名...
[TBLPROPERTIES (property_name=property_value, ...)];--设置表的属性
