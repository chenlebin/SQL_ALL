--b、分桶表优化
--	构建普通emp表
use db_df2;
--创建普通表
create table tb_emp01
(
    empno     string,
    ename     string,
    job       string,
    managerid string,
    hiredate  string,
    salary    double,
    jiangjin  double,
    deptno    string
) row format delimited fields terminated by '\t';
--加载数据
load data local inpath '/root/hivedata/emp01.txt' into table tb_emp01;

select *
from tb_emp01;

--	构建分桶emp表
-- 创建分桶表
create table tb_emp02
(
    empno     string,
    ename     string,
    job       string,
    managerid string,
    hiredate  string,
    salary    double,
    jiangjin  double,
    deptno    string
)
    clustered by (deptno) sorted by (deptno asc) into 3 buckets
    row format delimited fields terminated by '\t';

-- 创建分桶表tb_emp03并且使用ORC索引row group index 和blomm fitter index
create table tb_emp03
(
    empno     string,
    ename     string,
    job       string,
    managerid string,
    hiredate  string,
    salary    double,
    jiangjin  double,
    deptno    string
)
    clustered by (deptno) sorted by (deptno asc) into 3 buckets
    row format delimited fields terminated by '\t'
    stored as orc tblproperties (
    "orc.create.index" = "true",
    "orc.bloom.filter.columns" = "deptno"
    );

insert into tb_emp03
select *
from tb_emp01
    distribute by deptno
    sort by deptno asc;

select *
from tb_emp03;

//向上取整
desc function extended ceil;

--查询各部门的平均工资
explain extended
select ceil(sum(salary) / count(*)) as pj, deptno
from tb_emp03
group by deptno sort by pj desc;

explain extended
select sum(salary) / count(*) as pj, deptno
from tb_emp03
group by deptno;


-- 写入分桶表
insert overwrite table tb_emp02
select *
from tb_emp01;

--	构建普通dept表
create table tb_dept01
(
    deptno string,
    dname  string,
    loc    string
)
    row format delimited fields terminated by ',';
-- 加载数据
load data local inpath '/root/hivedata/dept01.txt' into table tb_dept01;

select *
from tb_dept01;


--	构建分桶dept表
create table tb_dept02
(
    deptno string,
    dname  string,
    loc    string
)
    clustered by (deptno) sorted by (deptno asc) into 3 buckets
    row format delimited fields terminated by ',';

--写入分桶表
insert overwrite table tb_dept02
select *
from tb_dept01;


--	构建分桶dept表并且使用ORC索引row group index 和blomm fitter index
create table tb_dept03
(
    deptno string,
    dname  string,
    loc    string
)
    clustered by (deptno) sorted by (deptno asc) into 3 buckets
    row format delimited fields terminated by ','
    stored as orc tblproperties (
    "orc.create.index" = "true",
    "orc.bloom.filter.columns" = "deptno"
    );

insert into tb_dept03
select *
from tb_dept01
    distribute by deptno
    sort by deptno;

select *
from tb_dept03
where deptno > 10;

explain
select *
from tb_dept03
where deptno > 10;


--普通join的执行优化
explain extended
select a.empno,
       a.ename,
       a.salary,
       b.deptno,
       b.dname
from tb_emp01 a
         join tb_dept01 b on a.deptno = b.deptno;

--	分桶的Join执行计划
--开启分桶SMB(Sort-Merge-Buket) join
set hive.optimize.bucketmapjoin = true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin.sortedmerge = true;


select *
from tb_emp02;

select *
from tb_dept02;

--查看执行计划
0

