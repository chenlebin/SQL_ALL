--根据上述需求，这种情况下是无法根据group by分组聚合实现的，因为分组聚合只能实现返回一条聚合的结果，
--但是需求中需要每个部门返回薪资最高的前两名，有两条结果，这时候就需要用到窗口函数中的分区来实现了。

--7369	SMITH	CLERK	7902	1980-12-17	800.00		20
create table if not exists t_anli3_topN
(
    empno     int comment "员工id",
    ename     STRING comment "员工名称",
    job       string comment "岗位",
    managerid int comment "领导id",
    diredate  string comment "入职日期",
    salary    float comment "薪水",
    bonus     float comment "奖金",
    deptno    int comment "部门编号"
)
    row format delimited
        fields terminated by '\t';


load data local inpath "/root/hivedata/emp.txt" into table t_anli3_topN;

select *
from t_anli3_topN;

--创建临时表
create table t_anli3_topN_tmp
as
select empno,
       ename,
       job,
       managerid,
       diredate,
       salary,
       bonus,
       deptno,
       salary + nvl(bonus, 0) as xinzi
from t_anli3_topN;

select *
from t_anli3_topN_tmp;

select *
from (
         select *,
                row_number() over (partition by deptno order by xinzi desc) as r1
         from t_anli3_topN_tmp) tmp
where tmp.r1 <= 2;












