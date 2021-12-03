----------Hive join----------
use db_df2;
--todo 1、inner join
select e.id, e.name, e_a.city, e_a.street
from employee e
         inner join employee_address e_a
                    on e.id = e_a.id;
--等价于 inner join=join
select e.id, e.name, e_a.city, e_a.street
from employee e
         join employee_address e_a
              on e.id = e_a.id;

--等价于 隐式连接表示法
select e.id, e.name, e_a.city, e_a.street
from employee e,
     employee_address e_a
where e.id = e_a.id;

--todo 2、left join
select e.id, e.name, e_conn.phno, e_conn.email
from employee e
         left join employee_connection e_conn
                   on e.id = e_conn.id;

--等价于 left outer join
select e.id, e.name, e_conn.phno, e_conn.email
from employee e
         left outer join employee_connection e_conn
                         on e.id = e_conn.id;


--todo 3、right join
select e.id, e.name, e_conn.phno, e_conn.email
from employee e
         right join employee_connection e_conn
                    on e.id = e_conn.id;

--等价于 right outer join
select e.id, e.name, e_conn.phno, e_conn.email
from employee e
         right outer join employee_connection e_conn
                          on e.id = e_conn.id;


--todo 4、full outer join
select e.id, e.name, e_a.city, e_a.street
from employee e
         full outer join employee_address e_a
                         on e.id = e_a.id;
--等价于
select e.id, e.name, e_a.city, e_a.street
from employee e
         full join employee_address e_a
                   on e.id = e_a.id;


select *
from employee;
select *
from employee_address;

--todo 5、left semi join
select *
from employee e
         left semi
         join employee_address e_addr
              on e.id = e_addr.id;

--相当于 inner join,但是只返回左表全部数据， 只不过效率高一些
select e.*
from employee e
         inner join employee_address e_addr
                    on e.id = e_addr.id;

--todo 6、cross join
--下列A、B、C 执行结果相同，但是效率不一样：
--A:
select a.*, b.*
from employee a,
     employee_address b
where a.id = b.id;
--B:
select *
from employee a
         cross join employee_address b on a.id = b.id;
select *
from employee a
         cross join employee_address b
where a.id = b.id;
--C:
select *
from employee a
         inner join employee_address b on a.id = b.id;

--一般不建议使用方法A和B，因为如果有WHERE子句的话，往往会先生成两个表行数乘积的行的数据表然后才根据WHERE条件从中选择。
--因此，如果两个需要求交集的表太大，将会非常非常慢，不建议使用。


explain
select a.*, b.*
from employee a,
     employee_address b
where a.id = b.id;
--B:
explain
select *
from employee a
         cross join employee_address b on a.id = b.id;
--C:
explain
select *
from employee a
         inner join employee_address b on a.id = b.id;