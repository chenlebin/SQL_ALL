------  两种变量
declare
    i     number := 50;
    pemp  emp%rowtype;--记录型变量（存行数据）   变量名.字段名
    pname emp.ename%type;--引用型变量（存列数据）
begin

    --select * from emp;
    --输出语句
    dbms_output.put_line(i);

    --记录型变量的使用
    select * into pemp from emp where empno = 7499;
    dbms_output.put_line('工号：' || pemp.empno || '岗位：' || pemp.job);

    --引用型变量
    select ename into pname from emp where sal > 3500;
    dbms_output.put_line(pname);


end;



------ 流程控制语句 if
-- if(表达式) then 
-- PLSQL语句 
-- elsif PLSQL语句 
-- elsif PLSQL语句 
-- ... else PLSQL语句
-- end if;
declare
    i number(2) := 15;
begin
    if i < 10 then
        dbms_output.put_line(i || '小于10');
    else
        dbms_output.put_line(i || '大于10');
    end if;
end;



------ 流程控制语句 loop
-- loop
-- PLSQL语句
-- exit when (表达式) 
-- end loop;

---- 特殊
-- 表达式为TURE时退出循环
-- 表达式为FALSE时继续循环
declare
    i number(2) := 11;
begin
    loop
        dbms_output.put_line(i);
        i := i - 1;
        exit when i < 10;
    end loop;
end;



------ 流程控制语句 while
-- while(表达式) loop
-- PLSQL语句
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



------ 流程控制语句 for
-- for i in [reverse] 下限值...上限值 loop
-- PLSQL语句
-- end loop;

---- 注意 
-- 1 左右均为闭区间[下限值,上限值]
-- 2 reverse这个可选参数的意思是倒序（从大到小2->1）默认为正序
declare
    j number(2) := 0;--全局变量
begin
    for i in 1..1000
        loop
            -- 这里的i是局部变量
            dbms_output.put_line(i || j);
        end loop;
end;



