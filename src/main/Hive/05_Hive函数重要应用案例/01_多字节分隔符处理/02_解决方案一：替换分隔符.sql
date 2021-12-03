--time：2021-11-30-14:03
/*   思路：
将数据文件中的多字节分隔符进行数据替换，将多字节分隔符替换为单字节分隔符，
     使用MapReduce编程，编写一个MR程序进行字符替换。
*/
--step1 编写一个mapper类
--在类中使用replaceall方法将多字节分隔符替换为单字节分隔符

--step2 编写一个Driver类
--使用set设置mapper类等各项属性

--step3 执行driver类
--设置好input output

--step4 将output中mr执行过后的文件上传HDFS
--将output中的文件（part-m-00000）上传到hdfs对应的表所在的位置，将文件映射成表