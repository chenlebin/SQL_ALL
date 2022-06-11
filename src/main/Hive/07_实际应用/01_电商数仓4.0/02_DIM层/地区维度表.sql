--因为地区维度表基本不发生变化所以不进行分区处理，每行数据是一个省份
--数据装载是直接将两张ODS层的表进行join之后筛选需要的字段即可
--建表语句
DROP TABLE IF EXISTS dim_base_province;
CREATE EXTERNAL TABLE dim_base_province
(
    `id`            STRING COMMENT 'id',
    `province_name` STRING COMMENT '省市名称',
    `area_code`     STRING COMMENT '地区编码',
    `iso_code`      STRING COMMENT 'ISO-3166编码，供可视化使用',
    `iso_3166_2`    STRING COMMENT 'IOS-3166-2编码，供可视化使用',
    `region_id`     STRING COMMENT '地区id',
    `region_name`   STRING COMMENT '地区名称'
) COMMENT '地区维度表'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dim/dim_base_province/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


--数据装载，join ODS层两张表的数据之后选择选择需要的字段进行insert
insert overwrite table dim_base_province
select bp.id,
       bp.name,
       bp.area_code,
       bp.iso_code,
       bp.iso_3166_2,
       bp.region_id,
       br.region_name
from ods_base_province bp
         join ods_base_region br on bp.region_id = br.id;




