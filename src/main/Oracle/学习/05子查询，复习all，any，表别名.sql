-- 部门编号为30的员工、
select *
from emp
where deptno = 30;

--查询工资大于部门30的所有员工的工资信息
--all参照最高的值  
select deptno, ename, sal
from emp
where sal > all (select sal from emp where deptno = 30)
  and deptno <> 30;

select max(sal)
from emp
where deptno = 30;

--any参照最低的值
select deptno, ename, sal
from emp
where sal > any (select sal from emp where deptno = 30);

select min(sal)
from emp
where deptno = 30;

-- 查询各部门工资
select *
from emp
order by job;

-- 查询平均工资
select avg(sal)
from emp;
-- 查询工资大于  同岗位  平均工资的员工信息
-- f 代表给表取了个别名 
select empno, ename, job, sal
from emp f
where sal > (select avg(sal) from emp where job = f.job)
order by job;
-- 在子查询当中再加上一个where条件 让sal只比较自己同部门的平均工资

       
