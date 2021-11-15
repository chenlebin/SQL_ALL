/*多表关联操作*/
-- 别名显示
select empno 工号, ename 姓名, deptno 部门
from emp;
select *
from dept;

-- 如何关联
select e.empno 工号, e.ename 姓名, e.deptno 部门, d.dname 部门名称
from emp e,
     dept d;

-- 以no 作为条件限定关联   查询岗位为CLERK 的各部门和对应的部门名称
select e.empno 工号, e.ename 姓名, e.deptno 部门, d.dname 部门名称, job 岗位
from emp e,
     dept d
where e.deptno = d.deptno
  and e.job = 'CLERK';

select *
from emp;




