--todo parse_url_tuple函数是Hive中提供的基于parse_url的url解析函数，可以通过一次指定多个参数，
--    从URL解析出多个参数的值进行返回多列，函数为特殊的一对多函数类型，即通常所说的UDTF函数类型。
-- parse_url(url, 参数1,参数2,参数3,......)
drop table if exists tb_url;
--建表
create table tb_url
(
    id  int,
    url string
) row format delimited
    fields terminated by '\t';
--加载数据
load data local inpath '/root/hivedata/url.txt' into table tb_url;

select *
from tb_url;

select parse_url_tuple(url, "HOST", "PATH") as (host, path)
from tb_url;

select parse_url_tuple(url, "PROTOCOL", "HOST", "PATH") as (protocol, host, path)
from tb_url;

select parse_url_tuple(url, "HOST", "PATH", "QUERY") as (host, path, query)
from tb_url;


--parse_url_tuple
--todo 报错，因为parse_url_tuple函数时UDTF函数，查询出的数据为一个虚拟表中的数据，不能再
--     查询原表tb_url中的数据，因为无法查询一个表返回两张表的数据，
--     这个时候就需要配合侧视图 later view进行使用
select id,
       parse_url_tuple(url, "HOST", "PATH", "QUERY") as (host, path, query)
from tb_url;

-- 使用侧视图later view
select id, host, path, query
from tb_url tb lateral view parse_url_tuple(tb.url, "HOST", "PATH", "QUERY") parse as host, path, query;