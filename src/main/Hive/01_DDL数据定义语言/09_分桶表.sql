/*
time:2021-11-4-11:21
todo 分桶语法：clustered by (id) into 3 buckets
 id 分桶字段
 3  分桶个数
*/

use db_df2;
show tables;

drop table db_df2.wzry_all_hero;


create table db_df2.wzry_all_hero
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
    clustered by (id) --sorted by (id desc )
        into 3 buckets
    row format delimited
        fields terminated by '\t';


alter table wzry_all_hero
    clustered by (id) into 3 buckets;


select *
from wzry_all_hero;





















