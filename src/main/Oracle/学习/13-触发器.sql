---�������������ƶ�һ����������������ɾ�Ĳ�����ʱ��
----ֻҪ����ù����Զ�������������á�
----��伶����������������for each row�Ĵ�������
----�м���������������for each row�ľ����м���������
-----------��for each row��Ϊ��ʹ��:old����:new�������һ�м�¼��

--׼��
create table person
(
    pid   number,
    pname varchar2(50)
);

insert into person
values (1, 'wang');
insert into person
values (2, 'Liu');

---��伶������
----����һ����¼�����һ����Ա����ְ
create or replace trigger t1
    after
        insert
    on person
declare

begin
    dbms_output.put_line('һ����Ա����ְ');
end;
---����t1
insert into person
values (1, 'С��');
commit;
select *
from person;

---�м��𴥷���
---���ܸ�Ա����н
---raise_application_error(-20001~-20999֮��, '������ʾ��Ϣ');
create or replace trigger t2
    before
        update
    on emp
    for each row
declare

begin
    if :old.sal > :new.sal then
        raise_application_error(-20001, '���ܸ�Ա����н');
    end if;
end;
----����t2
select *
from emp
where empno = 7788;
update emp
set sal=sal - 1
where empno = 7788;
commit;


----������ʵ���������������м���������
---���������û������������֮ǰ���õ�������������ݣ�
------���������е������и�ֵ��

create sequence s_person;
select s_person.nextval
from dual;
#�������м�����

create or replace trigger auid
    before
        insert
    on person
    for each row
declare

begin
    select s_person.nextval into :new.pid from dual;
end;
--��ѯperson������
select *
from person;
---ʹ��auidʵ����������
insert into person (pname)
values ('a');
commit;
insert into person
values (1, 'b');
commit;
