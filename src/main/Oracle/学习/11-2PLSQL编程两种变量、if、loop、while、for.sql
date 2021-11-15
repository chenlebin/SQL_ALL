------  ���ֱ���
declare
    i     number := 50;
    pemp  emp%rowtype;--��¼�ͱ������������ݣ�   ������.�ֶ���
    pname emp.ename%type;--�����ͱ������������ݣ�
begin

    --select * from emp;
    --������
    dbms_output.put_line(i);

    --��¼�ͱ�����ʹ��
    select * into pemp from emp where empno = 7499;
    dbms_output.put_line('���ţ�' || pemp.empno || '��λ��' || pemp.job);

    --�����ͱ���
    select ename into pname from emp where sal > 3500;
    dbms_output.put_line(pname);


end;



------ ���̿������ if
-- if(���ʽ) then 
-- PLSQL��� 
-- elsif PLSQL��� 
-- elsif PLSQL��� 
-- ... else PLSQL���
-- end if;
declare
    i number(2) := 15;
begin
    if i < 10 then
        dbms_output.put_line(i || 'С��10');
    else
        dbms_output.put_line(i || '����10');
    end if;
end;



------ ���̿������ loop
-- loop
-- PLSQL���
-- exit when (���ʽ) 
-- end loop;

---- ����
-- ���ʽΪTUREʱ�˳�ѭ��
-- ���ʽΪFALSEʱ����ѭ��
declare
    i number(2) := 11;
begin
    loop
        dbms_output.put_line(i);
        i := i - 1;
        exit when i < 10;
    end loop;
end;



------ ���̿������ while
-- while(���ʽ) loop
-- PLSQL���
-- end loop;
declare
    i number(2) := 0;
begin
    while (i <= 10)
        loop
            dbms_output.put_line(i);
            i := i + 1;
        end loop;
end;



------ ���̿������ for
-- for i in [reverse] ����ֵ...����ֵ loop
-- PLSQL���
-- end loop;

---- ע�� 
-- 1 ���Ҿ�Ϊ������[����ֵ,����ֵ]
-- 2 reverse�����ѡ��������˼�ǵ��򣨴Ӵ�С2->1��Ĭ��Ϊ����
declare
    j number(2) := 0;--ȫ�ֱ���
begin
    for i in 1..1000
        loop
            -- �����i�Ǿֲ�����
            dbms_output.put_line(i || j);
        end loop;
end;



