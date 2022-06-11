//1》只选择使用的字段
//由于采用列式存储，选择需要的字段可加快字段的读取、减少数据量。避免采用*读取所有字段。
[GOOD]:
SELECT time, user, host
FROM tbl
    [BAD]:
SELECT *
FROM tbl

     //2》过滤条件必须加上分区字段
     //对于有分区的表，where语句中优先使用分区字段进行过滤。acct_day是分区字段，visit_time是具体访问时间。
    [GOOD]:
SELECT time, user, host
FROM tbl
where acct_day=20171101
    [BAD]:
SELECT *
FROM tbl
where visit_time = 20171101

    //3》 Group By语句优化
    //合理安排Group by语句中字段顺序对性能有一定提升。将Group By语句中字段按照每个字段distinct数据多少进行降序排列。
    [GOOD] :
SELECT
GROUP BY uid, gender
    [BAD]:
SELECT
GROUP BY gender,
         uid

             //4》 Order by时使用Limit
             //Order by需要扫描数据到单个worker节点进行排序，导致单个worker需要大量内存。如果是查询Top N或者Bottom N，使用limit可减少排序计算和内存压力。
             [GOOD] :
SELECT *
FROM tbl
ORDER BY time
LIMIT 100 [BAD]:
SELECT *
FROM tbl
ORDER BY time

    //5》使用Join语句时将大表放在左边
    //Presto中join的默认算法是broadcast join，即将join左边的表分割到多个worker，然后将join右边的表数据整个复制一份发送到每个worker进行计算。如果右边的表数据量太大，则可能会报内存溢出错误。
    [GOOD]
SELECT... FROM large_table l join small_table s
on l.id = s.id
    [BAD]
SELECT... FROM small_table s join large_table l
on l.id = s.id