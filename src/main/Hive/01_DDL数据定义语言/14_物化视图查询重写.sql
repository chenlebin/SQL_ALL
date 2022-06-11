--设置事务表需要的属性，其实不用设置了因为我在Hive-site.xml文件中已经配置过了
set hive.support.concurrency = true; --Hive是否支持并发
set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动线程和清理线程
set hive.compactor.worker.threads = 1;
--在此metastore实例上运行多少个压缩程序工作线程。
--1、新建一张事务表 student_trans
CREATE TABLE student_trans
(
    sno   int,
    sname string,
    sdept string
)
    --以sno字段作为分桶字段 分桶个数为2桶
    clustered by (sno) into 2 buckets
    stored as orc
    TBLPROPERTIES ('transactional' = 'true');


select *
from db_df2.student;


--2、导入数据到student_trans中
insert overwrite table student_trans
select num, name, dept
from db_df2.student;

describe formatted db_df2.student;


describe formatted db_df2.student_trans;

select *
from student_trans;

insert into student_trans
values (114514, '仙贝', 'MA');

--3、对student_trans建立【聚合物化视图】
CREATE MATERIALIZED VIEW student_trans_agg
AS
SELECT sdept, count(*) as sdept_cnt
from student_trans
group by sdept;

--注意 这里当执行CREATE MATERIALIZED VIEW，会启动一个MR对物化视图进行构建
--可以发现当下的数据库中有了一个物化视图
show tables;

show materialized views;
--当数据源更新时（删除delete或者修改modify）需要对物化视图进行重构rebuid
alter materialized view student_trans_agg rebuild;

select *
from student_trans_agg;

--4、对原始表student_trans查询
--由于会命中物化视图，重写query查询物化视图，查询速度会加快（没有启动MR，只是普通的table scan）
explain extended
SELECT sdept, count(*) as sdept_cnt
from student_trans
group by sdept;


-- 关闭查询重写功能
alter materialized view db_df2.student_trans_agg DISABLE rewrite;
-- 开启查询重写功能
alter materialized view db_df2.student_trans_agg enable rewrite;


--5、查询执行计划可以发现 查询被自动重写为TableScan alias: itcast.student_trans_agg
--转换成了对物化视图的查询  提高了查询效率
explain
SELECT sdept, count(*) as sdept_cnt
from student_trans
group by sdept;


show materialized views;

describe formatted db_df2.student_trans_agg
