---�洢����
--�洢���̣��洢���̾�����ǰ�Ѿ�����õ�һ��pl/sql���ԣ����������ݿ��
--------����ֱ�ӱ����á���һ��pl/sqlһ�㶼�ǹ̶������ҵ��
-- ��1---��ָ��Ա����100��Ǯ
create or replace procedure p1(eno emp.empno%type)
    is -- is �� as������, ��������������� �൱��declare.

begin
    update emp set sal=sal + 100 where empno = eno;
    commit;
end;

select *
from emp
where empno = 7788;
----����p1
declare

begin
    p1(7788);
end;

----�洢���̣��з���ֵ�ĺ�����
--��2: --ͨ���洢����ʵ�ּ���ָ��Ա������н
----�洢����(����3)�ʹ洢�����Ĳ��������ܴ����� 
----�洢�����ķ���ֵ���Ͳ��ܴ�����

--ע�⣺return����������Ͳ��ܴ��г���
create or replace function f_yearsal(eno emp.empno%type) return number --��һ��return
    is
    s number(10); --ָ��return��ı������� ��������
begin
    --commΪ���� comm=nullʱΪ0�� nvl():���������ʽ����һ���� null ֵ��
    select sal * 12 + nvl(comm, 0) into s from emp where empno = eno;
    return s; --�ڶ���return
end;
