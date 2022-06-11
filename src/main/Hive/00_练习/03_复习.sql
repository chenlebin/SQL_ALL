drop table db_df2.game_list;
create external table if not exists db_df2.game_list
(
    id           int comment '编号',
    name         string comment '游戏名称',
    type         string comment '游戏类型',
    grade        double comment '游戏评分',
    size         double comment '游戏大小 单位G',
    series       string comment '是否属于哪个系列',
    start_time   string comment '开始时间',
    end_time     string comment '结束时间',
    playing_time int comment '实际游戏时长 单位小时',
    year         int comment '在哪年玩的'
) comment '玩过的游戏清单'
    --partitioned by (year int comment '在哪年玩的')
--clustered by (id) sorted by (id) into 3 buckets
    row format delimited
        fields terminated by ','
--stored as orc
    location '/user/hive/warehouse/db_df2.db/game_list';


set hive.exec.max.dynamic.partitions=1000; --允许最大的动态分区

set hive.exec.max.dynamic.partitions.pernode=100; --单个节点允许最大分区

set hive.exec.dynamic.partition=true; --启动动态分区

set hive.exec.dynamic.partition.mode=nonstrict; --动态分区为非严格模式


insert overwrite table db_df2.game_list
values (1, '侠客风云传', '武侠', 9, 5, '侠客风云传', '2015-09-01', '2018-10-12', 300, 2015),
       (2, '侠客风云传前传', '武侠', 8, 20, '侠客风云传', '2021-12-11', '2021-12-22', 100, 2021),
       (3, '原神', 'RPG', 7.5, 10, '无', '2020-09-05', '2022-05-07', 1362, 2020),
       (4, '我的勇者', 'RPG', 6, 4, '无', '2018-06-22', '2020-12-07', 1500, 2016),
       (5, '艾尔登法环', '魂类', 10, 50, '魂系列', '2022-04-18', '2022-05-01', 150, 2022),
       (6, '黑帝斯', '肉鸽', 8, 10, '无', '2021-12-12', '2021-12-26', 120, 2021),
       (7, '黑暗之魂3', '魂类', 7, 25, '魂系列', '2022-05-02', '2022-05-04', 10, 2022),
       (8, '怪物猎人物语1', '物语', 9, 4, '怪物猎人', '2017-07-12', '2017-08-03', 200, 2017),
       (9, '怪物猎人物语2', '物语', 8.5, 20, '怪物猎人', '2022-01-16', '2022-01-27', 140, 2022),
       (10, '怪物猎人:世界', '动作', 10, 70, '怪物猎人', '2022-01-28', '2022-02-17', 300, 2022),
       (11, '怪物猎人:XX', '动作', 9, 20, '怪物猎人', '2022-04-03', '2022-04-14', 120, 2022),
       (12, '暗黑破坏神3', '刷子', 7, 24, '暗黑破坏神', '2022-01-01', '2022-01-18', 160, 2022),
       (13, '鬼谷八荒', '动作', 6, 15, '无', '2021-07-13', '2022-07-25', 130, 2021),
       (14, '英雄联盟', '多人竞技', 8, 30, '英雄联盟', '2013-05-28', '2021-12-22', 2000, 2013),
       (15, '英雄联盟手游', '多人竞技', 6, 5, '英雄联盟', '2021-08-12', '2021-09-05', 120, 2022),
       (16, '王者荣耀', '多人竞技', 7.5, 15, '无', '2015-06-15', '2022-05-07', 1000, 2015),
       (17, '奥拉星', '宝可梦', 8, 5, '无', '2012-02-12', '2016-09-05', 800, 2012),
       (18, '泰拉瑞亚', '沙盒', 10, 4, '无', '2017-11-18', '2021-07-30', 300, 2017),
       (19, '宝可梦剑盾', '宝可梦', 8.5, 20, '宝可梦', '2022-01-30', '2022-02-21', 160, 2022),
       (20, '宝可梦:绿宝石', '宝可梦', 10, 0.03, '宝可梦', '2013-07-12', '2013-07-20', 50, 2013);


LOAD DATA INPATH '/hivedata/game_list.txt' overwrite INTO TABLE db_df2.game_list;



select *
from game_list
where year = 2022;

--创建分区表，以年份进行分区
create external table if not exists db_df2.game_list_p
(
    id           int comment '编号',
    name         string comment '游戏名称',
    type         string comment '游戏类型',
    grade        double comment '游戏评分',
    size         double comment '游戏大小 单位G',
    series       string comment '是否属于哪个系列',
    start_time   string comment '开始时间',
    end_time     string comment '结束时间',
    playing_time int comment '实际游戏时长 单位小时'
) comment '玩过的游戏清单'
    partitioned by (year int comment '在哪年玩的')
--clustered by (id) sorted by (id) into 3 buckets
    row format delimited
        fields terminated by ',';

insert into table game_list_p partition (year)
select *
from game_list;

select *
from game_list_p;


--todo 复习Hive中查看元数据的show语法

show tblproperties game_list;

show functions;

show partitions game_list;

show create table game_list;

show columns from game_list;

show schemas;

show databases;

show tables;

show views;

show materialized views;

desc formatted game_list;

explain extended
select *
from game_list
where year = 2022;

desc database db_df2;




























