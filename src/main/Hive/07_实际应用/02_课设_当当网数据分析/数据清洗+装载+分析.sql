-- 示例
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
    clustered by (id) into 3 buckets
    row format delimited
        fields terminated by '\t';

create table db_df2.keshe_ts_dangdang
(
    `ts_ranking` int comment '图书排名',
    `ts_name`    string comment '图书名称',
    `ts_writer`  string comment '图书作者',
    `ts_time`    string comment '出版时间',
    `ts_press`   string comment '出版社',
    `ts_price`   int comment '图书价格'
) comment '当当网图书销售排名表'
    row format delimited
        fields terminated by '\t';

show databases;

select *
from keshe_ts_dangdang;

-- 尝试分析出出版图书最多的前十家出版社
select ts_press, count(*)
from keshe_ts_dangdang
group by ts_press
order by count(*) desc
limit 10;

-- 进行数据清洗并按照图书排名进行查询操作
select ts_ranking     as `图书排名`,
       --使用if语句判断，如果包含注释（）则使用正则表达式进行数据清洗，不包含注释则使用正常的书籍名
       if(ts_name like '%（%',
          regexp_extract(ts_name, '(.*)[（](.*)', 1)
           , ts_name) as `图书名称`,
       ts_writer      as `图书作者`,
       ts_time        as `出版时间`,
       ts_press       as `出版社`,
       ts_price       as `图书价格`
from keshe_ts_dangdang
order by ts_ranking;

select *
from keshe_ts_dangdang;

-- 尝试分析出哪些年出版的图书最畅销Top 10
select year(ts_time)
from keshe_ts_dangdang;

select year(ts_time), count(*)
from keshe_ts_dangdang
group by year(ts_time)
order by count(*) desc
limit 10;
;


-- 将进行完数据清洗的数据进行清洗，并创建新表进行存储，之后将新表数据导出回MySQL进行数据可视化
drop table if exists db_df2.keshe_ts_dangdang_xin;

create table db_df2.keshe_ts_dangdang_xin
(
    `ts_ranking` int comment '图书排名',
    `ts_name`    string comment '图书名称',
    `ts_writer`  string comment '图书作者',
    `ts_year`    string comment '出版年份',
    `ts_time`    string comment '出版时间',
    `ts_press`   string comment '出版社',
    `ts_price`   int comment '图书价格'
) comment '当当网图书销售排名表'
    row format delimited
        fields terminated by '\t';

-- 进行数据装载
insert overwrite table keshe_ts_dangdang_xin
select ts_ranking,
       --使用if语句判断，如果包含注释（）则使用正则表达式进行数据清洗，不包含注释则使用正常的书籍名
       if(ts_name like '%（%',
          regexp_extract(ts_name, '(.*)[（](.*)', 1)
           , ts_name),
       ts_writer,
       year(ts_time),
       ts_time,
       ts_press,
       ts_price
from keshe_ts_dangdang
order by ts_ranking
;


select *
from keshe_ts_dangdang_xin;



show databases;



show databases;