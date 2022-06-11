--todo MapReduce属性优化
-- todo 1 本地模式(智能自动判断是 本地模式 还是提交到 集群上运行 )  [已在hive-site.xml中配置写死 开启 ]
--使用Hive的过程中，有一些数据量不大的表（HDFS中映射的文件）也会转换为MapReduce处理，提交到集群时，需要申请资源，
--等待资源分配，启动JVM进程，再运行Task，一系列的过程比较繁琐，本身数据量并不大，提交到YARN运行返回会导致性能较差的问题。
--Hive为了解决这个问题，延用了MapReduce中的设计，提供本地计算模式，允许程序不提交给YARN，直接在本地运行，
--以便于提高小数据量程序的性能。

-- 开启本地模式
set hive.exec.mode.local.auto = true;
--todo 本地模式的条件（需满足所有条件）
--1> 表总数据量<128M
--2> MapTask<4个
--3> ReduceTask=0||1个


-- todo 2 JVM复用 (hadoop3.0以上版本弃用)
--Hadoop默认会为每个Task启动一个JVM来运行，而在JVM启动时内存开销大；
--Job数据量大的情况，如果单个Task数据量比较小，也会申请JVM，这就导致了资源紧张及浪费的情况；
--JVM重用可以使得JVM实例在同一个job中重新使用N次，当一个Task运行结束以后，JVM不会进行释放，
--而是继续供下一个Task运行，直到运行了N个Task以后，就会释放；
--N的值可以在Hadoop的mapred-site.xml文件中进行配置，通常在10-20之间。

-- Hadoop3之前的配置，在mapred-site.xml中添加以下参数
-- todo Hadoop3中已不再支持该选项
set mapreduce.job.jvm.numtasks=10;


-- todo 3 并行执行(智能自动判断 如果stage之间没有依赖关系则并行执行)  [已在hive-site.xml中配置写死 开启 ]
-- Hive在实现HQL计算运行时，会解析为多个Stage，有时候Stage彼此之间有依赖关系，只能挨个执行，
--但是在一些别的场景下，很多的Stage之间是没有依赖关系的；
-- 例如Union语句，Join语句等等，这些Stage没有依赖关系，但是Hive依旧默认挨个执行每个Stage，
--这样会导致性能非常差，我们可以通过修改参数，开启并行执行，当多个Stage之间没有依赖关系时，
--允许多个Stage并行执行，提高性能。

-- 开启Stage并行化，默认为false
SET hive.exec.parallel=true;
-- 指定并行化线程数，默认为8
SET hive.exec.parallel.thread.number=16;



