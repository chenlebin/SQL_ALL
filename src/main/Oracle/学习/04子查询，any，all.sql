/*�Ӳ�ѯ������������Ӳ�ѯ���
max����ѯ���ֵ
min����ѯ��Сֵ
*/
select *
from emp
where sal > (select min(sal) from emp)
  and sal < (select max(sal) from emp);

--ת��Ϊbetween and(between and ��ʵ��>=��<=)
select *
from emp
where sal between (select min(sal) from emp)
          and (select max(sal) from emp);

/*�Ӳ�ѯ  
in :���ѯ�᳢�����Ӳ�ѯ����е��κ�һ���������ƥ�䣬ֻҪ��һ��ƥ��ɹ��������ѯ����
��ǰ�����ļ�¼

any :any����������뵥�в��������ʹ�ã����ҷ�����ֻҪƥ���Ӳ�ѯ���κ�һ���������

all :all����������뵥����������ʹ�ã����ҷ����б���ƥ�������Ӳ�ѯ���
*/

/*��ѯ������sales���ŵ�Ա����Ϣ*/
select *
from emp
where deptno in (select deptno from dept where dname <> 'SALES');
/*������*/
select *
from dept;

/*��emp���в�ѯ���ʴ��ڲ��ű��Ϊ10������һ��Ա�����ʵ��������ŵ�Ա����Ϣ*/
select *
from emp
where sal
    > any
      (select sal from emp where deptno = 10)
  and deptno <> 10;

/*��emp���в�ѯ���ʴ��ڲ��ű��Ϊ30������Ա����Ϣ*/
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

/*��emp����ʹ�ù����Ӳ�ѯ��ѯ���ʴ���ְͬλ��ƽ�����ʵ�Ա����Ϣ*/
select *
from emp f
where sal > (select avg(sal)
             from emp
             where job = f.job)
order by sal desc;
                           




