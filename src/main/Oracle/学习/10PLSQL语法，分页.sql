--�������������ĺ��������������Ĳ����
declare
    --��������
    a int := 100;
    b int := 200;
    c number;


begin
    c := (a + b) / (a - b);
    dbms_output.put_line(c);


exception
    when zero_divide then
        dbms_output.put_line('��������Ϊ��');

end;

set serveroutput on
declare
    age int := 55;
begin
    if age >= 60 then
        dbms_output.put_line('���������������ˣ�');
    else
        dbms_output.put_line('��С��60�겻����������');
    end if;
end;


--������ڵ���60����������ݣ���������
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



-- ��ҳ
-- ��¼���� 14
-- ÿҳ��ʾ��¼�� 3
-- ��ҳ�� 5

-- rownum ����ӵ����е��Ⱥ�˳��
select rownum, t.*
from (select rownum, e.* from emp e order by sal desc) t
where rownum <= 3;

select rownum, t.*
from (select rownum, e.* from emp e order by sal desc) t
where rownum <= 6
  and rownum > 3;

-- ��ʾ��һҳ  ���з�һҳ
select p.*
from (select rownum r, t.*
      from (select e.* from emp e order by sal desc) t) p
where r <= 5
  and r > 0;


-- ��ʾ�ڶ�ҳ
select *
from (select rownum r, t.*
      from (select e.* from emp e order by sal desc) t) p
where r <= 6
  and r > 3;
             
             
             

