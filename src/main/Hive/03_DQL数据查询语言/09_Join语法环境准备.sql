--Join语法练习 建表
use db_df2;
drop table if exists employee_address;
drop table if exists employee_connection;
drop table if exists employee;

--table1: 员工表
CREATE TABLE employee
(
    id     int,
    name   string,
    deg    string,
    salary int,
    dept   string
) row format delimited
    fields terminated by ',';

--table2:员工家庭住址信息表
CREATE TABLE employee_address
(
    id     int,
    hno    string,
    street string,
    city   string
) row format delimited
    fields terminated by ',';

--table3:员工联系方式信息表
CREATE TABLE employee_connection
(
    id    int,
    phno  string,
    email string
) row format delimited
    fields terminated by ',';

--加载数据到表中
load data local inpath '/root/hivedata/employee.txt' into table employee;
load data local inpath '/root/hivedata/employee_address.txt' into table employee_address;
load data local inpath '/root/hivedata/employee_connection.txt' into table employee_connection;

select *
from employee;

select *
from employee_address;

select *
from employee_connection;