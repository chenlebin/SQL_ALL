/*����ѯ�ص�*/
-- 1 �ص�
-- 1>�ѿ�����, �������ű���һ�ű�����п��ܵ����ȫ�����г�����
-- ���γɵѿ�������Ȼ����ͨ�� on֮��������޶���ѯ�������
select *
from emp;
select *
from dept;

-- ͨ������������ӵѿ��������ҳ���Ҫ��ѯ������
select e.empno, e.ename, e.deptno, d.dname
from emp e,
     dept d
where e.deptno = d.deptno;
select e.empno, e.ename, e.deptno, d.dname
from dept d,
     emp e;


-- ������ inner join...on ��������
select *
from emp e
         inner join dept d on e.deptno = d.deptno
order by empno;

-- �����ֶ�
insert into emp(empno, ename, job)
values (9547, '������', '����');


-- �������� left join...on ��������
select *
from emp e
         left join dept d on e.deptno = d.deptno
order by empno;
-- �������� right join...on ��������                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
select *
from emp e
         right join dept d on e.deptno = d.deptno
order by empno;
-- ��ȫ������ full join...on ��������
select *
from emp e
         full join dept d on e.deptno = d.deptno
order by empno;




