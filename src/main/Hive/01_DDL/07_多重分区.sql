-----todo 多重分区表
--单分区表，按省份分区
create table t_user_province
(
    id   int,
    name string,
    age  int
) partitioned by (province string);
--双分区表，按省份和市分区
--todo 分区字段之间是一种递进的关系 因此要注意分区字段的顺序 谁在前在后
create table t_user_province_city
(
    id   int,
    name string,
    age  int
) partitioned by (province string, city string);

--todo 双分区表的数据加载 静态分区加载数据
load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
    partition (province = 'zhejiang',city = 'hangzhou');
load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
    partition (province = 'zhejiang',city = 'ningbo');
load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
    partition (province = 'shanghai',city = 'pudong');

--双分区表的使用  使用分区进行过滤 减少全表扫描 提高查询效率
select *
from t_user_province_city
where province = "zhejiang"
  and city = "hangzhou";






























