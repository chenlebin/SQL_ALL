--编写一个hive自定义函数的流程：

--1 写一个java类继承UDF,并重载evaluate方法，方法中实现函数的业务逻辑
----- 重载意味着可以在一个java类中实现多个函数功能

--2 将程序打成jar包，上传到HS2服务器本地或者HDFS

--3 客户端命令行中添加jar包到Hive的classpath：
add jar /root/hive-udf-1.0-SNAPSHOT.jar;

--4 将这个jar注册为临时函数（给UDF命名）：
create temporary function jphone as "cn.itcast.hive.udf.EncryptPhoneNumber";

--5 使用函数：jphone()
select jphone(13577960831);



select jphone(1123232234);


select jphone("sdfasfertg");