-- ������ռ���û���

SQL> create
tablespace wang datafile 'D:\wang.dbf' size 10m extent management local uniform size 256k;

--��ռ��Ѵ�����

SQL> create
user wang identified by wang default tablespace wang;

--�û��Ѵ�����

SQL> grant dba to wang;

--��Ȩ�ɹ���

--�﷨��
1--�����û�
create
user itheima
identified by itheima
default tablespace itheima;

--���û���Ȩ
--oracle���ݿ��г��ý�ɫ
connect--���ӽ�ɫ��������ɫ
resource--�����߽�ɫ
dba--��������Ա��ɫ
--��itheima�û�����dba��ɫ
grant dba to itheima;

---�л���itheima�û���

2----oracle�еķ�ҳ
---rownum�кţ���������select������ʱ��
--ÿ��ѯ��һ�м�¼���ͻ��ڸ����ϼ���һ���кţ�
--�кŴ�1��ʼ�����ε��������������ߡ�

----���������Ӱ��rownum��˳��
select rownum, e.*
from emp e
order by e.sal desc
----����漰�����򣬵��ǻ�Ҫʹ��rownum�Ļ������ǿ����ٴ�Ƕ�ײ�ѯ��
select rownum, t.*
from (
         select rownum, e.*
         from emp e
         order by e.sal desc) t;

��
:  ��ҳ��ʽ�Ƶ�����:
select *
from emp
order by sal desc

--��һҳ ע��: rownum > 3 ������
select rownum, e.*
from (select * from emp order by sal desc) e
where rownum <= 3

--����취: ��rownum���ñ�������=> select rownum r ...where r>3
select *
from (select rownum r, e.*
      from (select * from emp order by sal desc) e) e1
where r > 3

--�ڶ�ҳ
select *
from (select rownum r, e.*
      from (select * from emp order by sal desc) e) e1
where r > 3
  and r <= 6

--��һҳ  ��׼д��    
select *
from (select rownum r, e.*
      from (select * from emp order by sal desc) e) e1
where r > 0
  and r <= 3

--1.��ѯԱ������Ա�����ʽ��н����ѯ�������з�ҳȡ����һҳ��һҳ������¼
select *
from (select rownum r, e.* from (select * from emp order by sal desc) e) e1
where r > 0
  and r <= 3
/*
��ҳ��ʽ
pageNo = 1
pageSize = 3

select * from (select rownum r,e.* from (select * from ���� order by ���� desc)e) e1 
where r > (pageNo - 1)*pageSize and r <= pageNo*pageSize
*/

    ��. oracle��� pl/sql develope �����������

--����number����, ����PI����, �����ַ�����pjob
declare
i number :=160;
  PI
constant number := 3.14;
  pjob
varchar2(50);
begin
  dbms_output.put_line
(i);
  i
:= i+1;
  dbms_output.put_line
(PI);

select job
into pjob
from emp
where empno = 7788;
dbms_output
.
put_line
(pjob);
end;

--//��MySQL��༭���ݱ�:
--//����һ: �ڱ����ϵ��Ҽ�--�༭����/��������(���ݱ�)
--//������: ִ�����д����ɴ򿪽���������еı༭��,��һ����,�ٵ�����.
select *
from emp for update;


declare
pemp emp%rowtype;       --�����¼�ͱ���%rowtype(���ܴ��� һ�м�¼ ������)
  pname
emp.ename%type;   --���������ͱ���%type(���ܴ��� һ���е�ĳ�� ����)
begin
select *
into pemp
from emp
where empno = 7499; --into����¼�ͱ�����ֵ
dbms_output
.
put_line
('����: '||pemp.empno || ', ����:'||pemp.ename);

select ename
into pname
from emp
where empno = 7788;
dbms_output
.
put_line
(pname);
end;


--����number����������PI�����������¼�ͱ��������������ͱ���
declare
i number := 1;                --�������
  pjob
varchar2(50);            --�����ַ�
  PI
constant number := 3.14;   --���峣��
  pemp
emp%rowtype;             --�����¼�ͱ���
  pname
emp.ename%type;         --���������ͱ���
begin
select *
into pemp
from emp
where empno = 7499; --into����¼�ͱ�����ֵ
dbms_output
.
put_line
('Ա����ţ�'||pemp.empno || ',Ա��������'||pemp.ename);
  dbms_output.put_line
(i);
  --PI := PI + 1;
  dbms_output.put_line
(PI);
select ename
into pname
from emp
where empno = 7499;
select job
into pjob
from emp
where empno = 7499;
dbms_output
.
put_line
(pname);
  dbms_output.put_line
(pjob);
end;

select *
from emp for update;

--=======================================if��֧
/*
if�жϷ�֧�﷨��
begin
  if �ж����� then
  elsif �ж����� then
  else
  end if; 
end;
*/
--�ӿ���̨����һ�����֣����������1�����������1
declare
age number := &age; --&age�ӿ���̨����
begin
  if
age = 1 then
    dbms_output.put_line('����1');
else
    dbms_output.put_line('�Ҳ���1');
end if;
end;


--��������������18�����£����δ�����ˣ�18~40�������ˣ�40���� ������
declare
age number := &age;
begin
  if
age < 18 then
    dbms_output.put_line('δ������');
  elsif
age >=18 and age <= 40 then
    dbms_output.put_line('������');
else
    dbms_output.put_line('������');
end if;
end;

---pl/sql�е�loopѭ��
---�����ַ�ʽ���1��10�Ǹ�����
---whileѭ��
declare
i number(2) := 1;
begin
  while
i<11 loop
     dbms_output.put_line(i);
     i
:= i+1;
end loop;
end;
---exitѭ��
declare
i number(2) := 1;
begin
  loop
exit when i>10;
    dbms_output.put_line
(i);
    i
:= i+1;
end loop;
end;
---forѭ��
declare

begin
for i in 1..10 loop
     dbms_output.put_line(i);
end loop;
end;

---�α꣺���Դ�Ŷ�����󣬶��м�¼��
--���emp��������Ա��������
declare
cursor c1 is
select *
from emp; --�����α�c1 �������е�emp���е���Ϣ
emprow
emp%rowtype;       --������¼����(��)����������ѭ��������ÿ������
begin
open c1;
loop
fetch c1 into emprow;  --��ȡһ����¼(��)����emprow������
         exit
when c1%notfound; --���α�(�����)�м�¼Ϊ��ʱ�˳�ѭ��
         dbms_output.put_line
(emprow.ename);
end loop;
close c1;
end;



-----��ָ������Ա���ǹ���
declare
cursor c2(eno emp.deptno%type)
  is
select empno
from emp
where deptno = eno;
en
emp.empno%type;
begin
open c2(10);
loop
fetch c2 into en;
        exit
when c2%notfound;
update emp
set sal=sal + 100
where empno = en;
commit;
end loop;
close c2;
end;

----��ѯ10�Ų���Ա����Ϣ
select *
from emp
where deptno = 10;


---�洢����
--�洢���̣��洢���̾�����ǰ�Ѿ�����õ�һ��pl/sql���ԣ����������ݿ��
--------����ֱ�ӱ����á���һ��pl/sqlһ�㶼�ǹ̶������ҵ��
-��1---��ָ��Ա����100��Ǯ
create
or replace procedure p1(eno emp.empno%type)
is   -- is �� as������, ��������������� �൱��declare.

begin
update emp
set sal=sal + 100
where empno = eno;
commit;
end;

select *
from emp
where empno = 7788;
----����p1
declare

begin
  p1
(7788);
end;

----�洢���̣��з���ֵ�ĺ�����
--��2: --ͨ���洢����ʵ�ּ���ָ��Ա������н
----�洢����(����3)�ʹ洢�����Ĳ��������ܴ����� 
----�洢�����ķ���ֵ���Ͳ��ܴ�����

--ע�⣺return����������Ͳ��ܴ��г���
create
or replace function f_yearsal(eno emp.empno%type) return number --��һ��return
is
  s number(10);  --ָ��return��ı������� ��������   
begin
  --commΪ���� comm=nullʱΪ0�� nvl():���������ʽ����һ���� null ֵ��
select sal * 12 + nvl(comm, 0)
into s
from emp
where empno = eno;
return s; --�ڶ���return
end;

----����f_yearsal
----�洢�����ڵ��õ�ʱ�򣬷���ֵ��Ҫ���ա�
declare
s number(10);  --����ָ��һ����������return����ֵ
begin
  s
:= f_yearsal(7788);
  dbms_output.put_line
(s);
end;

---todo out���Ͳ������ʹ��  ����ʡȥreturn
--��3: --ʹ�ô洢����������н

--�βΣ� enoΪ���ű�ţ�yearsalΪȫ��н�� out:������� number�����Դ�����,��number(10)
create
or replace procedure p_yearsal(eno emp.empno%type, yearsal out number)
is
   s number(10); --��н�������ھֲ�������
   c
emp.comm%type; --���𣨺����ھֲ�������
begin
  --�����ֶζ�Ӧ��������: sal*12, nvl(comm, 0) into s, c
  -- sal*12 => s
  -- nvl(comm, 0) => c
select sal * 12, nvl(comm, 0)
into s, c
from emp
where empno = eno;
yearsal
:= s+c; --ȫ��н�� yearsal�õ���ֵ, out���Ͳ���, ����return����
end;

---����p_yearsal
declare
yearsal number(10); --ָ����н�Ĳ�������
begin
  p_yearsal
(7788, yearsal);
  dbms_output.put_line
(yearsal);
end;

----in��out���Ͳ�����������ʲô��
---�����д Ĭ��ֵΪin 
---�����漰��into��ѯ��丳ֵ����:=��ֵ�����Ĳ���(ע���ǲ���,���Ǳ���)��������ʹ��out�����Ρ�


---�洢���̺ʹ洢����������
---�﷨���𣺹ؼ��ֲ�һ����
------------�洢�����ȴ洢���̶�������return��
---�������𣺴洢�����з���ֵ�����洢����û�з���ֵ��
----------����洢������ʵ���з���ֵ��ҵ�����Ǿͱ���ʹ��out���͵Ĳ�����
----------�����Ǵ洢����ʹ����out���͵Ĳ���������Ҳ����������˷���ֵ��
----------�����ڴ洢�����ڲ���out���Ͳ�����ֵ����ִ����Ϻ�����ֱ���õ�������Ͳ�����ֵ��

----���ǿ���ʹ�ô洢�����з���ֵ�����ԣ����Զ��庯����
----���洢���̲��������Զ��庯����
----�������󣺲�ѯ��Ա��������Ա�����ڲ������ơ�

----����׼����������scott�û��µ�dept���Ƶ���ǰ�û��¡�
create table dept as
select *
from scott.dept;

---1-ʹ�ô�ͳ��ʽ��ʵ�ְ�������
----todo �������󣺲�ѯ��Ա��������Ա�����ڲ������ơ�
select e.ename, d.dname
from emp e,
     dept d
where e.deptno = d.deptno;

---2-ʹ�ô洢������ʵ���ṩһ�����ű�ţ����һ���������ơ�������ѯ dept��,emp��
create
or replace function fdna(dno dept.deptno%type) return dept.dname%type
is
  dna dept.dname%type; --�洢�����ڲ����������ڴ��벿������
begin
select dname
into dna
from dept
where deptno = dno; --dnoΪ�洢���̵Ĳ��������ű��
return dna;
end;

---3-ʹ��fdna�洢������ʵ�ְ������󣺲�ѯ��Ա��������Ա�����ڲ������ơ�
select e.ename, fdna(e.deptno)
from emp e;


---todo �������������ƶ�һ����������������ɾ�Ĳ�����ʱ��
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


select *
from person;

---��伶������
----����һ����¼�����һ����Ա����ְ
create
    or replace trigger t1
    after
        insert
    on person
declare

begin
  dbms_output.put_line
('һ����Ա����ְ');
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
create
or replace trigger t2
before
update
    on emp
    for each row
declare

begin
    if
        :old.sal > :new.sal then
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

create
or replace trigger auid
before
insert
on person
for each row
declare

begin
select s_person.nextval
into :new.pid
from dual;
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


----oracle10g    ojdbc14.jar
----oracle11g    ojdbc6.jar

--==========================����

--1.��������
create sequence emp_seq;
//��֤: �Ҳ�sequences�ļ���

--2.��β�ѯ���У�currval,nextval��
select emp_seq.nextval
from dual;
//��һ�ε��ñ���ʹ��nextval,��Ϊ���ݱ�����ֵ.���ִ�п���������ֵ

select emp_seq.currval
from dual;
//�鿴��ǰ����ֵ

--3.ɾ������
drop sequence emp_seq;

--4.Ϊemp��������ݣ�������Ϊid��ֵ
insert into emp(empno)
values (7777);
commit;

insert into emp(empno)
values (emp_seq.nextval);
//����ִ�пɲ��������¼
commit;

select *
from emp;

--5.���У�������MySql���Զ�����
create sequence seq_test
    start with 5 //��5��ʼ
increment by 2    //����2
maxvalue  20      //���ֵ20
cycle             //����20����ѭ��
cache 5           //����5

��:
create sequence seq_test
    start with 5
    increment by 2
    maxvalue 20
    cycle cache 5;

select seq_test.nextval
from dual;








