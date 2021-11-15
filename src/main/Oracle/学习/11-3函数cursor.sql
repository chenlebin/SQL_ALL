-----给指定部门员工涨工资
---cursor游标：可以存放多个对象，多行记录。
--输出emp表中所有员工的姓名
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
        commit;--提交操作，一旦出错回到上一步
    end loop;
    close c2;
end;

----查询10号部门员工信息
select *
from emp
where deptno = 10;
