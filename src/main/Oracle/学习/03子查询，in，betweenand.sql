/* ѡ����*/
select *
from emp
where sal > 1212
/*ע��*/
/* ƥ��ename��z��ͷ��������Ϣ*/
select *
from emp
where ename like 'z%'
/* ��ѯ����û�н����Ա����Ϣ*/
select *
from emp
where comm is null

select *
from emp
where ename like 'J%'

/* �Ӳ�ѯ   ����һ����ѯ�Ľ����Ϊ����*/
select *
from emp
where sal > (select min(sal) from emp)
  and sal < (select max(sal) from emp)

/*��Χ��ѯ  in(x,x,x)*/
select *
from emp
where job in ('MANAGER', 'CLERK')

select *
from emp
where job not in ('MANAGER', 'CLERK')

/*BETTWEEN  X AND  Y �ؼ���  ���൱�ڴ��ڵ���X С�ڵ���Y ��*/
select *
from emp
where sal between 2000 and 3000

/* ��ѯ����û�н����Ա����Ϣ*/
select *
from emp
where comm is null

/* ��ѯ�����н����Ա����Ϣ*/
select *
from emp
where comm is not null


