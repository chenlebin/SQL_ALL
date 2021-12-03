---------------------------------------Partition分区 DDL操作---------------------------------------
--1、增加分区
--step1: 创建表 手动加载分区数据
drop table if exists t_user_province;
create table if not exists t_user_province
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
) partitioned by (province string)
    row format delimited
        fields terminated by ',';
use db_df2;

load data local inpath '/root/hivedata/students.txt' into table t_user_province partition (province = "SH");

select *
from db_df2.t_user_province;
--step2：添加一个分区
alter table t_user_province
    drop if exists partition (province = "BJ");

ALTER TABLE t_user_province
    ADD PARTITION (province = 'BJ') location
        '/user/hive/warehouse/db_df2.db/t_user_province/province=BJ';

show partitions db_df2.t_user_province;

desc formatted db_df2.t_user_province;
--step3:必须自己把数据加载到增加的分区中 hive不会帮你添加


----此外还支持一次添加多个分区
ALTER TABLE table_name
    ADD PARTITION (dt = '2008-08-08', country = 'us') location '/path/to/us/part080808'
        PARTITION (dt = '2008-08-09', country = 'us') location '/path/to/us/part080809';


--2、重命名分区
ALTER TABLE t_user_province
    PARTITION (province = "SH") RENAME TO PARTITION (province = "Shanghai");

--3、删除分区
ALTER TABLE table_name
    DROP [IF EXISTS] PARTITION (dt='2008-08-08', country='us');
ALTER TABLE table_name
    DROP [IF EXISTS] PARTITION (dt='2008-08-08', country='us') PURGE;
--直接删除数据 不进垃圾桶

--4、修复分区
MSCK [REPAIR] TABLE table_name [ADD / DROP / SYNC PARTITIONS];


--5、修改分区
--更改分区文件存储格式  orc等
ALTER TABLE table_name
    PARTITION (dt = '2008-08-09') SET FILEFORMAT file_format;
--更改分区位置  todo hdfs://mycluster/nixx
ALTER TABLE t_user_province
    PARTITION (province = 'Shanghai') SET LOCATION "hdfs://mycluster/user/hive/warehouse/db_df2.db/t_user_province/province=Shanghai";


-----MSCK 修复分区---------------
--Step1：创建分区表
create table t_all_hero_part_msck
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
) partitioned by (role string)
    row format delimited
        fields terminated by "\t";

--Step2：在linux上，使用HDFS命令创建分区文件夹
hadoop fs -mkdir -p /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=sheshou
hadoop fs -mkdir -p /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=tanke

--Step3：把数据文件上传到对应的分区文件夹下
hadoop fs -put archer.txt /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=sheshou
hadoop fs -put tank.txt /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=tanke

--Step4：查询表 可以发现没有数据
select *
from t_all_hero_part_msck;
--Step5：使用MSCK命令进行修复
--add partitions可以不写 因为默认就是增加分区
MSCK repair table t_all_hero_part_msck add partitions;


--Step1：直接使用HDFS命令删除分区表的某一个分区文件夹
hadoop fs -rm -r /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=sheshou

--Step2：查询发现还有分区信息
--因为元数据信息没有删除
show partitions t_all_hero_part_msck;

--Step3：使用MSCK命令进行修复
MSCK repair table t_all_hero_part_msck drop partitions;


--Step4：todo 使用MSCK sync命令进行修复
MSCK repair table db_df2.t_user_province sync partitions;

show partitions t_user_province;
from t_user_province
select *
union all
from t_user_province
select *
where province = "Shanghai";

desc formatted db_df2.trans_student;

show create database db_df2;

show create table db_df2.t_user_province;











