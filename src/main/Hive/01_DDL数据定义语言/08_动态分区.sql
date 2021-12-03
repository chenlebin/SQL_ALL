//todo 动态分区
--开启动态分区功能
set hive.exec.dynamic.partition=true;
--指定动态分区模式为<nonstrick>非严格模式和<strick>严格模式
--严格模式要求至少有一个分区为静态分区
set hive.exec.dynamic.partition.mode=nonstrict;

drop table db_df2.wzry_all_hero_partition;

create table db_df2.wzry_all_hero_partition
(
    id           int,
    name         string,
    hp_max       int,
    mp_max       int,
    attack_max   int,
    defense_max  int,
    attack_range string,
    role_main    string,
    role_assist  string
) comment '王者荣耀全英雄'
    partitioned by (role string comment "英雄定位")
    row format delimited
        fields terminated by '\t';

use db_df2;
select *
from db_df2.wzry_all_hero;

--执行动态分区插入
insert into table wzry_all_hero_partition partition (role) --注意这里 分区值并没有手动写死指定
select tmp.*, tmp.role_main
from wzry_all_hero tmp;

//查询分区后的结果
select *
from wzry_all_hero_partition;

select *
from db_df2.wzry_all_hero_partition
where role = 'warrior'
  and attack_max > 300;



























