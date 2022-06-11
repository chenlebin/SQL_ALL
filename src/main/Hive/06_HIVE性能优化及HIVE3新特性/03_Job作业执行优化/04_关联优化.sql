--todo 关联优化

--当一个程序中如果有一些操作彼此之间有关联性，是可以在一个MapReduce中实现的，
--但是Hive会不智能的选择，Hive会使用两个MapReduce来完成这两个操作。

--例如：当我们执行  select …… from table group by id order by id desc。
-- 该SQL语句转换为MapReduce时，我们可以有两种方案来实现：

-- 方案一
--第一个MapReduce做group by，经过shuffle阶段对id做分组
--第二个MapReduce对第一个MapReduce的结果做order by，经过shuffle阶段对id进行排序
-- 方案二
--因为都是对id处理，可以使用一个MapReduce的shuffle既可以做分组也可以排序

--在这种场景下，Hive会默认选择用第一种方案来实现，这样会导致性能相对较差；
--可以在Hive中开启关联优化，对有关联关系的操作进行解析时，可以尽量放在同一个MapReduce中实现，
--Hive会对job进行评估如果满足条件会尽量放在一个MR中进行
--配置 todo [已在hive-site.xml中配置写死 开启 ]
set hive.optimize.correlation=true;

