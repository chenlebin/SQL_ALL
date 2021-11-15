select *
from db_df2.t_user;

/*数据样式：*/
--1	后羿	5986	1784	396	336	remotely	archer
/*题目要求：*/
/*
字段含义:id、name(英雄名称)、hp_max(最大生命 )、mp_max (最大法力) 、attack_max(最高物攻)
defense_max (最大物防)、role_main(主要定位)、role_assist (次要定位)。
- 分析一下:字段都是基本类型，字段的顺序需要注意一下。
- 字段之间的分隔符是制表符，需要使用row format语法进行指定。

*/

drop table db_df2.wzry;

create table db_df2.wzry
(
    id          int comment "ID",
    name        varchar(255) comment '英雄名称',
    hp_max      int comment "最大生命值",
    mp_max      int comment '最大法力值',
    attack_max  int comment "最大攻击",
    defense_max int comment "最大防御",
    role_main   varchar(255) comment "主要定位",
    role_assist varchar(255)
) comment "次要定位"
    row format delimited fields terminated by '\t';

select *
from db_df2.wzry;




























