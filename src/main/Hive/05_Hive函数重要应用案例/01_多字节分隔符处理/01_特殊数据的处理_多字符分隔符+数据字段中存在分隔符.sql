--------------------------------多字节分隔符----------------------------------------------------

--针对双字节分隔符 采用默认的SerDe来处理
-- 01||周杰伦||中国||台湾||男||七里香
drop table db_df2.singer;
create table singer
(
    id       string,
    name     string,
    country  string,
    province string,
    gender   string,
    works    string
)
    row format delimited fields terminated by '||';

--加载数据
load data local inpath '/root/hivedata/test01.txt' into table singer;

select *
from singer;


--情况二：数据的字段中包含了分隔符
-- 192.168.88.134 [08/Nov/2020:10:44:32 +0800] "GET / HTTP/1.1" 404 951
drop table db_df2.apachelog;
create table apachelog
(
    ip     string,
    stime  string,
    mothed string,
    url    string,
    policy string,
    stat   string,
    body   string
)
    row format delimited fields terminated by ' ';

load data local inpath '/root/hivedata/apache_web_access.log' into table apachelog;


select *
from apachelog;

------------------------
--清洗完数据之后  使用|分隔符
create table singer_wash
(
    id       string,
    name     string,
    country  string,
    province string,
    gender   string,
    works    string
)
    row format delimited fields terminated by '|';

load data local inpath '/root/hivedata/test01_wash.txt' into table singer_wash;

select *
from singer_wash;

