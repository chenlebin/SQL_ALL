--显示所有的函数和运算符
show functions;
--查看运算符或者函数的使用说明
describe function +;
--使用extended 可以查看更加详细的使用说明
describe function extended count;

--1、创建表dual
create table dual
(
    id string
);
--2、加载一个文件dual.txt到dual表中
--dual.txt只有一行内容：内容为一个空格
load data local inpath '/root/hivedata/dual.txt' into table dual;
--3、在select查询语句中使用dual表完成运算符、函数功能测试
select 1 + 1
from dual;

select 1 + 1;
