-- ���ű��Ϊ30��Ա����
select *
from emp
where deptno = 30;

--��ѯ���ʴ��ڲ���30������Ա���Ĺ�����Ϣ
--all������ߵ�ֵ  
select deptno, ename, sal
from emp
where sal > all (select sal from emp where deptno = 30)
  and deptno <> 30;

select max(sal)
from emp
where deptno = 30;

--any������͵�ֵ
select deptno, ename, sal
from emp
where sal > any (select sal from emp where deptno = 30);

select min(sal)
from emp
where deptno = 30;

-- ��ѯ�����Ź���
select *
from emp
order by job;

-- ��ѯƽ������
select avg(sal)
from emp;
-- ��ѯ���ʴ���  ͬ��λ  ƽ�����ʵ�Ա����Ϣ
-- f �������ȡ�˸����� 
select empno, ename, job, sal
from emp f
where sal > (select avg(sal) from emp where job = f.job)
order by job;
-- ���Ӳ�ѯ�����ټ���һ��where���� ��salֻ�Ƚ��Լ�ͬ���ŵ�ƽ������

       
