--计算两个整数的和与这两个整数的差的商
declare
    --声明变量
    a int := 100;
    b int := 200;
    c number;


begin
    c := (a + b) / (a - b);
    dbms_output.put_line(c);


exception
    when zero_divide then
        dbms_output.put_line('除数不能为零');

end;

set serveroutput on
declare
    age int := 55;
begin
    if age >= 60 then
        dbms_output.put_line('您可以申请退休了！');
    else
        dbms_output.put_line('您小于60岁不能申请退休');
    end if;
end;


--年龄大于等于60岁可申请退休，否则不允许
declare
    i int := 0;
begin
    loop
        select *
        from (select rownum r, t.* from (select e.* from emp e order by sal desc) t) p
        where r <= i + 3
          and r > i;
        i := i + 3;
        exit when i > 15;
    end loop;
end;



-- 分页
-- 记录总数 14
-- 每页显示记录数 3
-- 总页数 5

-- rownum 是添加到表中的先后顺序
select rownum, t.*
from (select rownum, e.* from emp e order by sal desc) t
where rownum <= 3;

select rownum, t.*
from (select rownum, e.* from emp e order by sal desc) t
where rownum <= 6
  and rownum > 3;

-- 显示第一页  五行分一页
select p.*
from (select rownum r, t.*
      from (select e.* from emp e order by sal desc) t) p
where r <= 5
  and r > 0;


-- 显示第二页
select *
from (select rownum r, t.*
      from (select e.* from emp e order by sal desc) t) p
where r <= 6
  and r > 3;
             
             
             

