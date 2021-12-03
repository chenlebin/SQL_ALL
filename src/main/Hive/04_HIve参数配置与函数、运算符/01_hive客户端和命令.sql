-------Batch Mode 批处理模式-----------
--todo -e "sql语句"
$HIVE_HOME
/bin/hive -e 'show databases'

--todo -f sql文件名
cd ~
--编辑一个sql文件 里面写上合法正确的sql语句
vim hive.sql
show databases;
--执行 从客户端所在机器的本地磁盘加载文件
$HIVE_HOME
/bin/hive -f /root/hive.sql
--也可以从其他文件系统加载sql文件执行
$HIVE_HOME/bin/hive -f hdfs://<namenode>:<port>/hive-script.sql
$HIVE_HOME/bin/hive -f s3://mys3bucket/s3-script.sql

--todo -i 进入交互模式之前运行初始化脚本
$HIVE_HOME/bin/hive -i /home/my/hive-init.sql

-- 使用静默模式将数据从查询中转储到文件中
$HIVE_HOME/bin/hive -S -e 'select * from db_df2.student' > a.txt

#----------启动服务-------------------
--todo --hiveconf
--设置hive的参数
$HIVE_HOME/bin/hive --hiveconf hive.root.logger=DEBUG,console

--todo --service
--启动服务
$HIVE_HOME/bin/hive --service metastore
$HIVE_HOME/bin/hive --service hiveserver2


---------交互式模式运行
/export/server/hive/bin/hive
hive> show databases;
OK
default
itcast
itheima
Time taken: 0.028 seconds, Fetched: 3 row(s)
hive> use db_df2;
OK
Time taken: 0.027 seconds
hive> exit;

--启用hive动态分区，需要在hive会话中设置两个参数：
set
hive.exec.dynamic.partition=true;
set
hive.exec.dynamic.partition.mode=nonstrict;



--todo 登录beeline客户端：
$HIVE_HOME
/bin/beeline
--输入连接hiveserver2的命令：
! connect jdbc:hive2://node01:/10000
--输入登入的用户名：
root
