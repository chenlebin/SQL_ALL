/*����һ��100m�ı�ռ䣬����չ��СΪ�Զ�����
��οռ����ʽΪ�Զ��οռ����ʽ����������*/
create tablespace tbs_onetbs datafile 'G:\orcl_tablespace\tbs_onetbs.dbf'
    size 100 m
    extent management local autoallocate
    segment space management auto;

/*������:*/
create temporary tablespace tbl
    tempfile
    'G:\orcl_tablespace\tbs_twotbs.ora2' size 100 m
    autoextend on next 10 m maxsize 200 m
    extent management local autoallocate;


/*�����⣺����һ��ѧ�����Ա���в���ɾ����ѯ���µȲ���*/
create table student_one
(
    s_id    number(10)          not null primary key,--ѧ��
    s_name  char(10)            not null,            -- ����
    sex     number(2) default 1 not null,            --�Ա�
    x_id    number(14)          not null,            --ϵ����
    b_id    char(8)             not null,            --�༶���
    jd_date date                not null             --��������
) tablespace tbs_onetbs;


/*�޸ı�ṹ��*/
-- ���ӱ��ֶ�
alter table student_one
    add (zhuzhi char(40));
-- ɾ�����ֶ�
alter table student_one
    drop (b_id);
-- �޸ı��ֶ�
alter table student_one
    modify (sex number(2) default 0);



/*�Ա��е����ݽ��в�����*/
-- ��������
insert into student_one(s_id, s_name, sex, x_id, jd_date, zhuzhi)
values (19104007, '���ֱ�', 1, 111000, to_date('22-04-2001', 'dd-mm-yyyy'), '����ʡ�������ְ���');
insert into student_one(s_id, s_name, sex, x_id, jd_date, zhuzhi)
values (19104008, '����δ��', 1, 111000, to_date('22-04-2001', 'dd-mm-yyyy'), '����ʡ�������ְ���');
insert into student_one(s_id, s_name, sex, x_id, jd_date, zhuzhi)
values (19104009, '�غ���', 0, 111000, to_date('22-04-2001', 'dd-mm-yyyy'), '����ʡ�������ְ���');
-- ɾ������
delete student_one
where s_id = 19104007;
-- �������ݣ��޸ģ�
update student_one
set s_name='�紵ѩ'
where s_id = 19104009;
-- ��ѯ����
select *
from student_one
where s_id > 19104008
  and zhuzhi = '����ʡ�������ְ���';

select *
from student_one;

























