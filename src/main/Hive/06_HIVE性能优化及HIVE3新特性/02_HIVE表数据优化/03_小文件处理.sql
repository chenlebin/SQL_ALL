--todo 避免小文件生成  将输出的文件进行合并
--Hive的存储本质还是HDFS，HDFS是不利于小文件存储的，因为每个小文件会产生一条元数据信息，
--并且不利于MapReduce的处理，MapReduce中每个小文件会启动一个MapTask计算处理，导致资源的浪费，
--所以在使用Hive进行处理分析时，要尽量避免小文件的生成。

--Hive中提供了一个特殊的机制，可以自动的判断是否是小文件，如果是小文件可以自动将小文件进行合并。

-- 如果hive的程序，只有maptask，将MapTask产生的所有小文件进行合并
set hive.merge.mapfiles=true;
-- 如果hive的程序，有Map和ReduceTask,将ReduceTask产生的所有小文件进行合并
set hive.merge.mapredfiles=true;
-- 每一个合并的文件的大小（244M）
set hive.merge.size.per.task=256000000;
-- 平均每个文件的大小，如果小于这个值就会进行合并(15M)
set hive.merge.smallfiles.avgsize=16000000;

--todo 小文件合并 对输入的文件进行小文件合并
--如果遇到数据处理的输入是小文件的情况，怎么解决呢？

--Hive中也提供一种输入类CombineHiveInputFormat，用于将小文件合并以后，再进行处理。
-- 设置Hive中底层MapReduce读取数据的输入类：将所有文件合并为一个大文件作为输入
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;


