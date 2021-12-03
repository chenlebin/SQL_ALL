/*创建一个100m的表空间，其扩展大小为自动管理，
其段空间管理方式为自动段空间管理方式，代码如下*/
create tablespace tbs_onetbs datafile 'G:\orcl_tablespace\tbs_onetbs.dbf'
    size 100 m
    extent management local autoallocate
    segment space management auto;

/*第三题:*/
create temporary tablespace tbl
    tempfile
    'G:\orcl_tablespace\tbs_twotbs.ora2' size 100 m
    autoextend on next 10 m maxsize 200 m
    extent management local autoallocate;


/*第四题：创建一个学生表，对表进行插入删除查询更新等操作*/
create table student_one
(
    s_id    number(10)          not null primary key,--学号
    s_name  char(10)            not null,            -- 姓名
    sex     number(2) default 1 not null,            --性别
    x_id    number(14)          not null,            --系别编号
    b_id    char(8)             not null,            --班级编号
    jd_date date                not null             --建档日期
) tablespace tbs_onetbs;


/*修改表结构：*/
-- 增加表字段
alter table student_one
    add (zhuzhi char(40));
-- 删除表字段
alter table student_one
    drop (b_id);
-- 修改表字段
alter table student_one
    modify (sex number(2) default 0);



/*对表中的数据进行操作：*/
-- 插入数据
insert into student_one(s_id, s_name, sex, x_id, jd_date, zhuzhi)
values (19104007, '陈乐斌', 1, 111000, to_date('22-04-2001', 'dd-mm-yyyy'), '江西省抚州市乐安县');
insert into student_one(s_id, s_name, sex, x_id, jd_date, zhuzhi)
values (19104008, '东方未明', 1, 111000, to_date('22-04-2001', 'dd-mm-yyyy'), '江西省抚州市乐安县');
insert into student_one(s_id, s_name, sex, x_id, jd_date, zhuzhi)
values (19104009, '秦红殇', 0, 111000, to_date('22-04-2001', 'dd-mm-yyyy'), '江西省抚州市乐安县');
-- 删除数据
delete student_one
where s_id = 19104007;
-- 更新数据（修改）
update student_one
set s_name='风吹雪'
where s_id = 19104009;
-- 查询数据
select *
from student_one
where s_id > 19104008
  and zhuzhi = '江西省抚州市乐安县';

select *
from student_one;

























