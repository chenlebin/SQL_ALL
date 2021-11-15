/*多表查询特点*/
-- 1 特点
-- 1>笛卡尔积, ，讲两张表汇成一张表的所有可能的组合全部罗列出来。
-- 先形成笛卡尔积，然后在通过 on之类的条件限定查询出的语句
select *
from emp;
select *
from dept;

-- 通过添加条件，从笛卡尔积中找出需要查询的数据
select e.empno, e.ename, e.deptno, d.dname
from emp e,
     dept d
where e.deptno = d.deptno;
select e.empno, e.ename, e.deptno, d.dname
from dept d,
     emp e;


-- 内连接 inner join...on 连接条件
select *
from emp e
         inner join dept d on e.deptno = d.deptno
order by empno;

-- 插入字段
insert into emp(empno, ename, job)
values (9547, '大话西游', '娱乐');


-- 左外连接 left join...on 连接条件
select *
from emp e
         left join dept d on e.deptno = d.deptno
order by empno;
-- 右外连接 right join...on 连接条件                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
select *
from emp e
         right join dept d on e.deptno = d.deptno
order by empno;
-- 完全外连接 full join...on 连接条件
select *
from emp e
         full join dept d on e.deptno = d.deptno
order by empno;




