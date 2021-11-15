---存储过程
--存储过程：存储过程就是提前已经编译好的一段pl/sql语言，放置在数据库端
--------可以直接被调用。这一段pl/sql一般都是固定步骤的业务。
-- 例1---给指定员工涨100块钱
create or replace procedure p1(eno emp.empno%type)
    is -- is 或 as都可以, 在其后面声明变量 相当于declare.

begin
    update emp set sal=sal + 100 where empno = eno;
    commit;
end;

select *
from emp
where empno = 7788;
----测试p1
declare

begin
    p1(7788);
end;

----存储过程（有返回值的函数）
--例2: --通过存储函数实现计算指定员工的年薪
----存储过程(见例3)和存储函数的参数都不能带长度 
----存储函数的返回值类型不能带长度

--注意：return后的数据类型不能带有长度
create or replace function f_yearsal(eno emp.empno%type) return number --第一个return
    is
    s number(10); --指定return后的变量名称 变量类型
begin
    --comm为奖金， comm=null时为0， nvl():从两个表达式返回一个非 null 值。
    select sal * 12 + nvl(comm, 0) into s from emp where empno = eno;
    return s; --第二个return
end;
