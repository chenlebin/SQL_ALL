------------------
--todo 使用正则Regex来解析数据

--如果表已存在就删除表
drop table if exists singer;
--创建表
create table singer
(
    id       string,--歌手id
    name     string,--歌手名称
    country  string,--国家
    province string,--省份
    gender   string,--性别
    works    string
) --作品
--指定使用RegexSerde加载数据
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
        WITH SERDEPROPERTIES ("input.regex" = "([0-9]*)\\|\\|(.*)\\|\\|(.*)\\|\\|(.*)\\|\\|(.*)\\|\\|(.*)");

--加载数据
load data local inpath '/root/hivedata/test01.txt' into table singer;

select *
from db_df2.singer;

--解决字段中存在空格的问题：还是使用RegexSerDe正则表达式匹配：


--如果表存在，就删除表
drop table if exists apachelog;
--创建表
create table apachelog
(
    ip     string, --IP地址
    stime  string, --时间
    mothed string, --请求方式
    url    string, --请求地址
    policy string, --请求协议
    stat   string, --请求状态
    body   string  --字节大小
)
--指定使用RegexSerde加载数据
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
--指定正则表达式
        WITH SERDEPROPERTIES (
        "input.regex" = "([^ ]*) ([^}]*) ([^ ]*) ([^ ]*) ([^ ]*) ([0-9]*) ([^ ]*)"
        ) stored as textfile;


load data local inpath '/root/hivedata/apache_web_access.log' into table apachelog;

select *
from apachelog;

describe formatted apachelog;








