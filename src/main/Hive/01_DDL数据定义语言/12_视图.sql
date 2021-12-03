---todo             Hive View视图相关语法
--time：2021-11-5-10:56
/*
                      todo      概念
- Hive中的视图（view）是一种虚拟表，只保存定义，不实际存储数据。
- 通常从真实的物理表查询中创建生成视图，也可以从已经存在的视图上创建新视图。
- 创建视图时，将冻结 视图的架构 ，如果删除或更改基础表，则视图将失败。
- 视图是用来简化操作的，不缓冲记录，也没有提高查询性能。
- 视图可以修改视图结构(CDA)但不能修改表数据(CRUD)
*/

/*
                      todo 使用视图的好处
- 使用视图可以降低查询复杂度，可以把嵌套子查询变成一个视图优化查询语句
- 将真实表中的特定列数据根据需求生成一个视图提供给用户，保护数据隐私

*/
--hive中有一张真实的基础表t_usa_covid19
select *
from db_df2.t_usa_covid19;

--1、创建视图
--todo limit 表示只返回前5行
create view v_usa_covid19 as
select count_date, county, state, deaths
from t_usa_covid19
limit 5;

--能否从已有的视图中创建视图呢  可以的
create view v_usa_covid19_from_view as
select *
from v_usa_covid19
limit 2;

--2、显示当前已有的视图
show tables;
show views;
--hive v2.2.0之后支持

--3、视图的查询使用
select *
from v_usa_covid19;

--能否插入数据到视图中呢？
--不行 报错  SemanticException:A view cannot be used as target table for LOAD or INSERT
insert into v_usa_covid19
select count_date, county, state, deaths
from t_usa_covid19;

--4、查看视图定义
show create table v_usa_covid19;

--5、删除视图
drop view v_usa_covid19_from_view;
--6、更改视图属性
    alter view v_usa_covid19 set TBLPROPERTIES ('comment' = 'This is a view');
--7、更改视图定义
    alter view v_usa_covid19 as select county, deaths
                                from t_usa_covid19
                                limit 2;


--通过视图来限制数据访问可以用来保护信息（password）不被随意查询:
create table userinfo
(
    firstname string,
    lastname  string,
    ssn       string,
    password  string
);

create view safer_user_info as
select firstname, lastname
from userinfo;

show views;

--可以通过where子句限制数据访问，比如，提供一个员工表视图，只暴露来自特定部门的员工信息:
create table employee
(
    firstname  string,
    lastname   string,
    ssn        string,
    password   string,
    department string
);

create view techops_employee as
select firstname, lastname, ssn
from userinfo
where department = 'java';


--使用视图优化嵌套查询
from (
         select *
         from people
                  join cart
                       on (cart.pepople_id = people.id)
         where firstname = 'join'
     ) a
select a.lastname
where a.id = 3;

--把嵌套子查询变成一个视图
create view shorter_join as
select *
from people
         join cart
              on (cart.pepople_id = people.id)
where firstname = 'join';
--基于视图查询
select lastname
from shorter_join
where id = 3;


/*todo 视图练习部分*/
describe extended wzry_all_hero;


describe extended v_wzry_sheshou;

describe formatted wzry_all_hero;

describe formatted wzry_pifu;

select *
from wzry_all_hero;

select *
from wzry_pifu;


create view if not exists v_wzry_sheshou
as
select t.*, p.win_rate, p.skin_price
from wzry_all_hero t
         inner join wzry_pifu p on p.id = t.id;

select *
from v_wzry_sheshou;





























