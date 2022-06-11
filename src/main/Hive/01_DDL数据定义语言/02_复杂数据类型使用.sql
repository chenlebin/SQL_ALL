use db_df2;


drop table if exists db_df2.wzry_pifu;
/*
字段:id、name(英雄名称)、win_rate（胜率)、skin_price（皮肤及价格）;
分析一下:前3个字段原生数据类型、最后一个字段复杂类型map。
需要指定字段之间分隔符、集合元素之间分隔符、map kv之间分隔符。*/
/*数据样式：*/
//1,孙悟空,53,西部大镖客:288-大圣娶亲:888-全息碎片:0-至尊宝:888-地狱火:1688
create table db_df2.wzry_pifu
(
    id         int comment "ID",
    name       string comment "英雄名",
    win_rate   int comment "胜率",
    skin_price map<string,int> comment "皮肤及价格"
) comment "王者荣耀英雄皮肤表"
    row format delimited
        fields terminated by "," --字段之间的分隔符
        collection items terminated by "-" --集合之间的分隔符
        map keys terminated by ":"; --映射之间的分隔符

select *
from wzry_pifu;

create view wzry_pifu_view
as
select name, win_rate
from wzry_pifu
where win_rate > 53;

select *
from wzry_pifu_view;



