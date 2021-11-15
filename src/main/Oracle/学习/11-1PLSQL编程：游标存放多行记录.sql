/*游标*/
-- 数组型变量
-- 关键字： cursor 
-- 可以存放多个对象，多行记录。
-- 输出emp表中所有员工的姓名
declare
    cursor c1 is select *
                 from emp; --定义游标c1 存入所有的emp表中的信息
    emprow emp%rowtype; --创建记录类型(行)变量来存入循环遍历的每行数据
begin
    open c1;
    loop
        -- fetch 将查询到的所有数据中抽取一行赋值给emprow
        fetch c1 into emprow; --获取一条记录(行)存入emprow变量中
        exit when c1%notfound; --当游标(结果集)中记录为空时退出循环
        dbms_output.put_line(emprow.ename);
    end loop;
    close c1;
end;
