--ѡ�񣬿��Խ��ж��Ĳ�ѯ��--
select *
from emp;

-- ͶӰ����ѯ���ű������
select *
from dept,
     emp;

-- ͶӰ����ѯָ���е�ֵ
select empno
from emp;

-- ��ָ���������
select empno as "Ա�����", ename as "Ա������", job as "ְ��"
from emp;

-- ������ֵ
select sal * (1.1), sal
from emp;

-- ����������е��ظ���  distinct
select job
from emp;
select distinct job
from emp;


