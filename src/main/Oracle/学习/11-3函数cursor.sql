-----��ָ������Ա���ǹ���
---cursor�α꣺���Դ�Ŷ�����󣬶��м�¼��
--���emp��������Ա��������
declare
    cursor c2(eno emp.deptno%type)
        is select empno
           from emp
           where deptno = eno;
    en emp.empno%type;
begin
    open c2(10);
    loop
        fetch c2 into en;
        exit when c2%notfound;
        update emp set sal=sal + 100 where empno = en;
        commit;--�ύ������һ������ص���һ��
    end loop;
    close c2;
end;

----��ѯ10�Ų���Ա����Ϣ
select *
from emp
where deptno = 10;
