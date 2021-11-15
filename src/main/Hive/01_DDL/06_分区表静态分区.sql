/*创建一个王者荣耀英雄表，其中包含各个位置的英雄，之后通过分区操作将不同位置的英雄进行分区操作：
  字段示例：
  ID	英雄	最大生命	最大法力	最高物攻	最大物防	 攻击范围	   主要定位	    次要定位
  id	name	hp_max	    mp_max	   attack_max	defense_max	 attack_range	role_main	role_assist

  */
show databases;
use db_df2;


show tables;


create table db_df2.wzry_hero
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
    row format delimited
        fields terminated by '\t';

select *
from db_df2.wzry_hero_part;

//查询主要定位role_main为射手archer，并且最大生命大于6000的信息
select count(*)
from db_df2.wzry_hero_part
where role_main = 'archer'
  and hp_max > 6000;



//注意：todo 创建分区表的语法规则
// 分区字段【不能是表中已经存在的字段】，因为分区字段最终也会以虚拟字段的形式显示在表结构上
create table db_df2.wzry_hero_part
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
    partitioned by (role string)--分区规则定义
    row format delimited
        fields terminated by '\t';
select *
from db_df2.wzry_hero_part;


/*todo 分区表数据加载--静态分区*/
//静态分区指的是分区的属性值是由用户在加载数据的时候手动指定的
// todo 语法规则：
// load data [local] inpath '文件路径' into table 表名 partition (分区字段='分区值'.....);

load data inpath '/hivedata/wzry_hero/archer.txt' into table db_df2.wzry_hero_part partition (role = 'archer');
load data inpath '/hivedata/wzry_hero/assassin.txt' into table db_df2.wzry_hero_part partition (role = 'assassin');
load data inpath '/hivedata/wzry_hero/mage.txt' into table db_df2.wzry_hero_part partition (role = 'mage');
load data inpath '/hivedata/wzry_hero/support.txt' into table db_df2.wzry_hero_part partition (role = 'support');
load data inpath '/hivedata/wzry_hero/tank.txt' into table db_df2.wzry_hero_part partition (role = 'tank');
load data inpath '/hivedata/wzry_hero/warrior.txt' into table db_df2.wzry_hero_part partition (role = 'warrior');


select *
from db_df2.wzry_hero_part;


//查询主要定位role_main为射手archer，并且最大生命大于6000的信息
select *
from db_df2.wzry_hero_part
where role = 'archer'
  and hp_max > 6000;







































































































