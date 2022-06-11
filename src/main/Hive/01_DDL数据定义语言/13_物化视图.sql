/*
todo                             物化视图的创建语法
-todo 物化视图的目的：预处理数据，并暂存，方便后续查询使用，提高查询效率
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db_name.]materialized_view_name
    [DISABLE REWRITE] 关闭查询重写
    [COMMENT materialized_view_comment] 物化视图注释
    [PARTITIONED ON (col_name, ...)] 设置分区字段
    [CLUSTERED ON (col_name, ...) | DISTRIBUTED ON (col_name, ...) SORTED ON (col_name, ...)]
    [
    [ROW FORMAT row_format] 设置分隔符
    [STORED AS file_format] 设置存储文件类型
    | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)] 设置存储介质（存储的系统，默认HIve）
  ]
  [LOCATION hdfs_path] 设置映射文件所在的地址
  [TBLPROPERTIES (property_name=property_value, ...)] 设置属性
AS SELECT ...; 绑定的select查询语句


todo 例句：
CREATE MATERIALIZED VIEW druid_wiki_mv
            STORED AS 'org.apache.hadoop.hive.druid.DruidStorageHandler'
AS
SELECT __time, page, user, c_added, c_removed
FROM src;

todo 当数据源变更时物化视图也需要更新以确保数据一致性，目前需要用户主动，触发[ REBUID ]重构
todo   alter materialized view  [db_name.]物化视图名 rebuid;

todo 物化视图的drop和show操作
-- Drops a materialized view
DROP MATERIALIZED VIEW [db_name.]materialized_view_name;
-- Shows materialized views (with optional filters)
SHOW MATERIALIZED VIEWS [IN database_name];
-- Shows information about a specific materialized view
DESCRIBE [EXTENDED | FORMATTED] [db_name.]materialized_view_name;

*/
show materialized views

