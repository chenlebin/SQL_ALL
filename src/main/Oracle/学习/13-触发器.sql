---触发器，就是制定一个规则，在我们做增删改操作的时候，
----只要满足该规则，自动触发，无需调用。
----语句级触发器：不包含有for each row的触发器。
----行级触发器：包含有for each row的就是行级触发器。
-----------加for each row是为了使用:old或者:new对象或者一行记录。

--准备
create table person
(
    pid   number,
    pname varchar2(50)
);

insert into person
values (1, 'wang');
insert into person
values (2, 'Liu');

---语句级触发器
----插入一条记录，输出一个新员工入职
create or replace trigger t1
    after
        insert
    on person
declare

begin
    dbms_output.put_line('一个新员工入职');
end;
---触发t1
insert into person
values (1, '小红');
commit;
select *
from person;

---行级别触发器
---不能给员工降薪
---raise_application_error(-20001~-20999之间, '错误提示信息');
create or replace trigger t2
    before
        update
    on emp
    for each row
declare

begin
    if :old.sal > :new.sal then
        raise_application_error(-20001, '不能给员工降薪');
    end if;
end;
----触发t2
select *
from emp
where empno = 7788;
update emp
set sal=sal - 1
where empno = 7788;
commit;


----触发器实现主键自增。【行级触发器】
---分析：在用户做插入操作的之前，拿到即将插入的数据，
------给该数据中的主键列赋值。

create sequence s_person;
select s_person.nextval
from dual;
#创建序列见下文

create or replace trigger auid
    before
        insert
    on person
    for each row
declare

begin
    select s_person.nextval into :new.pid from dual;
end;
--查询person表数据
select *
from person;
---使用auid实现主键自增
insert into person (pname)
values ('a');
commit;
insert into person
values (1, 'b');
commit;
