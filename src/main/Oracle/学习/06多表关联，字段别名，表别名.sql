/*����������*/
-- ������ʾ
select empno ����, ename ����, deptno ����
from emp;
select *
from dept;

-- ��ι���
select e.empno  ����,
       e.ename  ����,
       e.deptno ����,
       d.dname  ��������
       --*
from emp e,
     dept d;

--todo �����ӣ���ʽ�� inner join
-- ��no ��Ϊ�����޶�����   ��ѯ��λΪCLERK �ĸ����źͶ�Ӧ�Ĳ�������
select e.empno ����, e.ename ����, e.deptno ����, d.dname ��������, job ��λ
from emp e,
     dept d
where e.deptno = d.deptno
  and e.job = 'CLERK'
order by ����;
--todo ȫ������ full join
/*�ȼ�����Ĵ��� todo ����Ϊ��ʽ����*/
select e.empno ����, e.ename ����, e.deptno ����, d.dname ��������, job ��λ
from emp e
         full join dept d on e.DEPTNO = d.DEPTNO
where e.job = 'CLERK'
order by ����;
--todo �������� left join
select e.empno ����, e.ename ����, e.deptno ����, d.dname ��������, job ��λ
from emp e
         left join dept d on e.DEPTNO = d.DEPTNO
where e.job = 'CLERK'
order by ����;


--todo ������ rigth join
select e.empno ����, e.ename ����, e.deptno ����, d.dname ��������, job ��λ
from emp e
         right join dept d on e.DEPTNO = d.DEPTNO
where e.job = 'CLERK'
order by ����;



select *
from emp;




