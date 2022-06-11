/*子查询用运算符连接子查询结果
max：查询最大值
min：查询最小值
*/
select *
from emp
where sal > (select min(sal) from emp)
  and sal < (select max(sal) from emp);

--转换为between and(between and 其实是>=和<=)
select *
from emp
where sal between (select min(sal) from emp)
          and (select max(sal) from emp);

/*子查询  
in :外查询会尝试与子查询结果中的任何一个结果进行匹配，只要有一个匹配成功，则外查询返回
当前检索的记录

any :any运算符必须与单行操作符结合使用，并且返回行只要匹配子查询的任何一个结果即可

all :all运算符必须与单行运算符结合使用，并且返回行必须匹配所有子查询结果
*/

/*查询不属于sales部门的员工信息*/
select *
from emp
where deptno in (select deptno from dept where dname <> 'SALES');
/*工作表*/
select *
from dept;

/*在emp表中查询工资大于部门编号为10的任意一个员工工资的其他部门的员工信息*/
select *
from emp
where sal
    > any
      (select sal from emp where deptno = 10)
  and deptno <> 10;

/*在emp表中查询工资大于部门编号为30的所有员工信息*/
select sal
from emp
where deptno = 30;

select *
from emp
where sal > any (select sal
                 from emp
                 where deptno = 30);

select *
from emp
where sal > all
      (select sal
       from emp
       where deptno = 30);

/*在emp表中使用关联子查询查询工资大于同职位的平均工资的员工信息*/
select *
from emp f
where sal > (select avg(sal)
             from emp
             where job = f.job)
order by sal desc;
                           




