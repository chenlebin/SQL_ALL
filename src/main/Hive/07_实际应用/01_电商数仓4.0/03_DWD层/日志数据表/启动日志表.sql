--建表语句
DROP TABLE IF EXISTS dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log
(
    `area_code`       STRING COMMENT '地区编码',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `os`              STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `entry`           STRING COMMENT 'icon手机图标 notice 通知 install 安装后启动',
    `loading_time`    BIGINT COMMENT '启动加载时间',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `open_ad_ms`      BIGINT COMMENT '广告总共播放时间',
    `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点',
    `ts`              BIGINT COMMENT '时间'
) COMMENT '启动日志表'
    PARTITIONED BY (`dt` STRING) -- 按照时间创建分区
    STORED AS PARQUET -- 采用parquet列式存储
    LOCATION '/warehouse/gmall/dwd/dwd_start_log' -- 指定在HDFS上存储位置
    TBLPROPERTIES ('parquet.compression' = 'lzo') -- 采用LZO压缩
;


insert overwrite table dwd_start_log partition (dt = '2020-06-14')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.start.entry'),
       get_json_object(line, '$.start.loading_time'),
       get_json_object(line, '$.start.open_ad_id'),
       get_json_object(line, '$.start.open_ad_ms'),
       get_json_object(line, '$.start.open_ad_skip_ms'),
       get_json_object(line, '$.ts')
from ods_log
where dt = '2020-06-14'
  and get_json_object(line, '$.start') is not null;--先过滤出启动日志的数据（也就是包含启动日志的line）
