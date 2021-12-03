-- 创建表空间和用户：

SQL> create
tablespace wang datafile 'D:\wang.dbf' size 10m extent management local uniform size 256k;

--表空间已创建。

SQL> create
user wang identified by wang default tablespace wang;

--用户已创建。

SQL> grant dba to wang;

--授权成功。

--语法：
1--创建用户
create
user itheima
identified by itheima
default tablespace itheima;

--给用户授权
--oracle数据库中常用角色
connect--连接角色，基本角色
resource--开发者角色
dba--超级管理员角色
--给itheima用户授予dba角色
grant dba to itheima;

---切换到itheima用户下

2----oracle中的分页
---rownum行号：当我们做select操作的时候，
--每查询出一行记录，就会在该行上加上一个行号，
--行号从1开始，依次递增，不能跳着走。

----排序操作会影响rownum的顺序
select rownum, e.*
from emp e
order by e.sal desc
----如果涉及到排序，但是还要使用rownum的话，我们可以再次嵌套查询。
select rownum, t.*
from (
         select rownum, e.*
         from emp e
         order by e.sal desc) t;

二
:  分页公式推导过程:
select *
from emp
order by sal desc

--第一页 注意: rownum > 3 将报错
select rownum, e.*
from (select * from emp order by sal desc) e
where rownum <= 3

--解决办法: 将rownum设置别名即可=> select rownum r ...where r>3
select *
from (select rownum r, e.*
      from (select * from emp order by sal desc) e) e1
where r > 3

--第二页
select *
from (select rownum r, e.*
      from (select * from emp order by sal desc) e) e1
where r > 3
  and r <= 6

--第一页  标准写法    
select *
from (select rownum r, e.*
      from (select * from emp order by sal desc) e) e1
where r > 0
  and r <= 3

--1.查询员工表，将员工工资进行降序查询，并进行分页取出第一页，一页三条记录
select *
from (select rownum r, e.* from (select * from emp order by sal desc) e) e1
where r > 0
  and r <= 3
/*
分页公式
pageNo = 1
pageSize = 3

select * from (select rownum r,e.* from (select * from 表名 order by 列名 desc)e) e1 
where r > (pageNo - 1)*pageSize and r <= pageNo*pageSize
*/

    二. oracle编程 pl/sql develope 面向过程语言

--定义number变量, 定义PI常量, 定义字符变量pjob
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

--//如MySQL般编辑数据表:
--//方法一: 在表名上点右键--编辑数据/导出数据(备份表)
--//方法二: 执行下列代码后可打开结果集窗口中的编辑锁,点一下锁,再点锁关.
select *
from emp for update;


declare
pemp emp%rowtype;       --定义记录型变量%rowtype(仅能存贮 一行记录 的数据)
  pname
emp.ename%type;   --定义引用型变量%type(仅能存贮 一列中的某个 数据)
begin
select *
into pemp
from emp
where empno = 7499; --into给记录型变量赋值
dbms_output
.
put_line
('工号: '||pemp.empno || ', 姓名:'||pemp.ename);

select ename
into pname
from emp
where empno = 7788;
dbms_output
.
put_line
(pname);
end;


--定义number变量，定义PI常量，定义记录型变量，定义引用型变量
declare
i number := 1;                --定义变量
  pjob
varchar2(50);            --定义字符
  PI
constant number := 3.14;   --定义常量
  pemp
emp%rowtype;             --定义记录型变量
  pname
emp.ename%type;         --定义引用型变量
begin
select *
into pemp
from emp
where empno = 7499; --into给记录型变量赋值
dbms_output
.
put_line
('员工编号：'||pemp.empno || ',员工姓名：'||pemp.ename);
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

--=======================================if分支
/*
if判断分支语法：
begin
  if 判断条件 then
  elsif 判断条件 then
  else
  end if; 
end;
*/
--从控制台输入一个数字，如果数字是1，则输出我是1
declare
age number := &age; --&age从控制台输入
begin
  if
age = 1 then
    dbms_output.put_line('我是1');
else
    dbms_output.put_line('我不是1');
end if;
end;


--如果输入的年龄在18岁以下，输出未成年人，18~40：成年人，40以上 老年人
declare
age number := &age;
begin
  if
age < 18 then
    dbms_output.put_line('未成年人');
  elsif
age >=18 and age <= 40 then
    dbms_output.put_line('中年人');
else
    dbms_output.put_line('老年人');
end if;
end;

---pl/sql中的loop循环
---用三种方式输出1到10是个数字
---while循环
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
---exit循环
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
---for循环
declare

begin
for i in 1..10 loop
     dbms_output.put_line(i);
end loop;
end;

---游标：可以存放多个对象，多行记录。
--输出emp表中所有员工的姓名
declare
cursor c1 is
select *
from emp; --定义游标c1 存入所有的emp表中的信息
emprow
emp%rowtype;       --创建记录类型(行)变量来存入循环遍历的每行数据
begin
open c1;
loop
fetch c1 into emprow;  --获取一条记录(行)存入emprow变量中
         exit
when c1%notfound; --当游标(结果集)中记录为空时退出循环
         dbms_output.put_line
(emprow.ename);
end loop;
close c1;
end;



-----给指定部门员工涨工资
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

----查询10号部门员工信息
select *
from emp
where deptno = 10;


---存储过程
--存储过程：存储过程就是提前已经编译好的一段pl/sql语言，放置在数据库端
--------可以直接被调用。这一段pl/sql一般都是固定步骤的业务。
-例1---给指定员工涨100块钱
create
or replace procedure p1(eno emp.empno%type)
is   -- is 或 as都可以, 在其后面声明变量 相当于declare.

begin
update emp
set sal=sal + 100
where empno = eno;
commit;
end;

select *
from emp
where empno = 7788;
----测试p1
declare

begin
  p1
(7788);
end;

----存储过程（有返回值的函数）
--例2: --通过存储函数实现计算指定员工的年薪
----存储过程(见例3)和存储函数的参数都不能带长度 
----存储函数的返回值类型不能带长度

--注意：return后的数据类型不能带有长度
create
or replace function f_yearsal(eno emp.empno%type) return number --第一个return
is
  s number(10);  --指定return后的变量名称 变量类型   
begin
  --comm为奖金， comm=null时为0， nvl():从两个表达式返回一个非 null 值。
select sal * 12 + nvl(comm, 0)
into s
from emp
where empno = eno;
return s; --第二个return
end;

----测试f_yearsal
----存储函数在调用的时候，返回值需要接收。
declare
s number(10);  --必须指定一个变量接收return返回值
begin
  s
:= f_yearsal(7788);
  dbms_output.put_line
(s);
end;

---todo out类型参数如何使用  可以省去return
--例3: --使用存储过程来算年薪

--形参： eno为部门编号，yearsal为全年薪资 out:输出类型 number不可以带长度,如number(10)
create
or replace procedure p_yearsal(eno emp.empno%type, yearsal out number)
is
   s number(10); --年薪（函数内局部变量）
   c
emp.comm%type; --奖金（函数内局部变量）
begin
  --二个字段对应二个变量: sal*12, nvl(comm, 0) into s, c
  -- sal*12 => s
  -- nvl(comm, 0) => c
select sal * 12, nvl(comm, 0)
into s, c
from emp
where empno = eno;
yearsal
:= s+c; --全年薪资 yearsal得到赋值, out类型参数, 类似return功能
end;

---测试p_yearsal
declare
yearsal number(10); --指定年薪的参数类型
begin
  p_yearsal
(7788, yearsal);
  dbms_output.put_line
(yearsal);
end;

----in和out类型参数的区别是什么？
---如果不写 默认值为in 
---凡是涉及到into查询语句赋值或者:=赋值操作的参数(注意是参数,不是变量)，都必须使用out来修饰。


---存储过程和存储函数的区别
---语法区别：关键字不一样，
------------存储函数比存储过程多了两个return。
---本质区别：存储函数有返回值，而存储过程没有返回值。
----------如果存储过程想实现有返回值的业务，我们就必须使用out类型的参数。
----------即便是存储过程使用了out类型的参数，起本质也不是真的有了返回值，
----------而是在存储过程内部给out类型参数赋值，在执行完毕后，我们直接拿到输出类型参数的值。

----我们可以使用存储函数有返回值的特性，来自定义函数。
----而存储过程不能用来自定义函数。
----案例需求：查询出员工姓名，员工所在部门名称。

----案例准备工作：把scott用户下的dept表复制到当前用户下。
create table dept as
select *
from scott.dept;

---1-使用传统方式来实现案例需求
----todo 案例需求：查询出员工姓名，员工所在部门名称。
select e.ename, d.dname
from emp e,
     dept d
where e.deptno = d.deptno;

---2-使用存储函数来实现提供一个部门编号，输出一个部门名称。（多表查询 dept表,emp表）
create
or replace function fdna(dno dept.deptno%type) return dept.dname%type
is
  dna dept.dname%type; --存储函数内部变量：用于存入部门名称
begin
select dname
into dna
from dept
where deptno = dno; --dno为存储过程的参数：部门编号
return dna;
end;

---3-使用fdna存储函数来实现案例需求：查询出员工姓名，员工所在部门名称。
select e.ename, fdna(e.deptno)
from emp e;


---todo 触发器，就是制定一个规则，在我们做增删改操作的时候，
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


select *
from person;

---语句级触发器
----插入一条记录，输出一个新员工入职
create
    or replace trigger t1
    after
        insert
    on person
declare

begin
  dbms_output.put_line
('一个新员工入职');
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


----oracle10g    ojdbc14.jar
----oracle11g    ojdbc6.jar

--==========================序列

--1.创建序列
create sequence emp_seq;
//验证: 右侧sequences文件夹

--2.如何查询序列（currval,nextval）
select emp_seq.nextval
from dual;
//第一次调用必须使用nextval,因为数据表中无值.多次执行可自增序列值

select emp_seq.currval
from dual;
//查看当前序列值

--3.删除序列
drop sequence emp_seq;

--4.为emp表插入数据，用序列为id赋值
insert into emp(empno)
values (7777);
commit;

insert into emp(empno)
values (emp_seq.nextval);
//连续执行可插入多条记录
commit;

select *
from emp;

--5.序列：类似于MySql的自动增长
create sequence seq_test
    start with 5 //从5开始
increment by 2    //步长2
maxvalue  20      //最大值20
cycle             //仅在20以内循环
cache 5           //缓存5

例:
create sequence seq_test
    start with 5
    increment by 2
    maxvalue 20
    cycle cache 5;

select seq_test.nextval
from dual;








