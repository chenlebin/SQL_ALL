----自定义InputFormat
add jar /root/HiveUserInputFormat.jar;


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
)
--指定使用分隔符为|
    row format delimited fields terminated by '|'
--指定使用自定义的类实现解析
    stored as
        inputformat 'bigdata.itcast.cn.hive.mr.UserInputFormat'
        outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

--加载数据
load data local inpath '/root/hivedata/test01.txt' into table singer;

select *
from db_df2.singer;

