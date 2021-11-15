--选择，可以进行多表的查询：--
select *
from emp;

-- 投影，查询两张表的数据
select *
from dept,
     emp;

-- 投影，查询指定列的值
select empno
from emp;

-- 给指定列起别名
select empno as "员工编号", ename as "员工名称", job as "职务"
from emp;

-- 计算列值
select sal * (1.1), sal
from emp;

-- 消除结果集中的重复行  distinct
select job
from emp;
select distinct job
from emp;


