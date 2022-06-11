/* 选择行*/
select *
from emp
where sal > 1212;

/*注释*/
/* 匹配ename以z开头的所有信息*/
select *
from emp
where ename like 'z%';

/* 查询表里没有奖金的员工信息*/
select *
from emp
where comm is null;

select *
from emp
where ename like 'J%';

/* 子查询   用另一个查询的结果作为条件*/
select distinct *
from emp
where sal > (select min(sal) from emp)
  and sal < (select max(sal) from emp)
  and comm is not null
order by comm;

select distinct job
from EMP;


/*范围查询  in(x,x,x)*/
select *
from emp
where job in ('MANAGER', 'CLERK');

select *
from emp
where job not in ('MANAGER', 'CLERK');

/*BETTWEEN  X AND  Y 关键字  （相当于大于等于X 小于等于Y ）*/
select *
from emp
where sal between 2000 and 3000;



