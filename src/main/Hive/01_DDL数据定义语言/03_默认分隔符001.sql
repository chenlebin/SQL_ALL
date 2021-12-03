/*题目要求*/
/*
字段:id、team_name (战队名称)、ace_player_name (王牌选手名字)
分析一下∶数据都是原生数据类型，且字段之间分隔符是\001，
因此在建表的时候可以省去row format语句，
因为hive默认的分隔符就是\001。
*/

/*数据样式*/
//1成都AG超玩会一诺
use db_df2;


drop table db_df2.wzry_team;

create table db_df2.wzry_team
(
    id              int comment "ID",
    team_name       string comment "战队名称",
    ace_player_name string comment "王牌选手名字"
)
--row format delimited
--fields terminated by "\001"
;


select *
from db_df2.wzry_team;

describe formatted db_df2.wzry_team;










