/*����������*/
-- ������ʾ
select empno ����, ename ����, deptno ����
from emp;
select *
from dept;

-- ��ι���
select e.empno ����, e.ename ����, e.deptno ����, d.dname ��������
from emp e,
     dept d;

-- ��no ��Ϊ�����޶�����   ��ѯ��λΪCLERK �ĸ����źͶ�Ӧ�Ĳ�������
select e.empno ����, e.ename ����, e.deptno ����, d.dname ��������, job ��λ
from emp e,
     dept d
where e.deptno = d.deptno
  and e.job = 'CLERK';

select *
from emp;




