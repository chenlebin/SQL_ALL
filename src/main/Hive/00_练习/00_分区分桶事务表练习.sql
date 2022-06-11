/*todo 练习部分*/

drop table if exists usa;
create external table if not exists usa
(
    sate   string comment "州",
    county string comment "县"
)
    comment "单词统计"
--clustered by (lie1) into 2 buckets
    row format delimited
        fields terminated by ","
    location "/MapReduce/mapjoin/cache";

select *
from usa;

create table if not exists usa_bucket
(
    sate   string comment "州",
    county string comment "县"
)
    comment "州县数据分桶表"
    clustered by (sate) into 2 buckets
    row format delimited
        fields terminated by ","
    stored as orc tblproperties
    ("orc.create.index" = "true","orc.bloom.filter.columns" = "stime,userid")
;

drop table if exists usa_bucket;

insert into usa_bucket
select *
from usa
    distribute by sate
    sort by sate;

--比较正常表和分通表之间的查询速度
select *
from usa
where sate = "Alabama";

select *
from usa_bucket
where sate = "Alabama";


create table if not exists usa_partition
(
    sate   string comment "州",
    county string comment "县"
)
    comment "州县数据分区表"
    partitioned by (p_sate string comment "分区_州")
    row format delimited
        fields terminated by ",";

insert into usa_partition
select tmp.*, tmp.sate
from usa tmp;

--比较正常表和分区表之间的查询速度
select *
from usa
where sate = "Alabama";

select *
from usa_partition
where p_sate = "Alabama";


explain extended
select *
from usa_partition
where p_sate = "Alabama";

-- 对表进行重命名
alter table usa_partition
    rename to usa_partition;
alter table usa_bucket
    rename to usa_bucket;
alter table usa
    rename to usa;

describe formatted usa;

describe formatted db_df2.usa_zong;

describe formatted usa_bucket;

describe formatted usa_partition;


show create table usa;

show create table usa_bucket;

show create table usa_partition;

-- 创建事务表并使用行式索引和bloom索引
drop table if exists usa_trans;
create table usa_trans
(
    sate   string comment "州",
    county string comment "县"
)
    comment "州县数据事务表"
    clustered by (county) into 4 buckets
    row format delimited
        fields terminated by ","
    stored as orc
    tblproperties (
        'transactional' = 'true',
        "orc.create.index" = "true",
        "orc.bloom.filter.columns" = "stime,userid");
-- 插入数据
insert into usa_trans
select *
from usa_bucket
    distribute by county
    sort by county;

desc formatted usa_trans;


insert into usa_trans
values ('xxx', 'sb');

select *
from usa_trans;


explain extended
update usa_trans
set sate = "xxx"
where county = 'Clarke';

explain extended
select *
from usa_trans
where sate = 'xxx';






