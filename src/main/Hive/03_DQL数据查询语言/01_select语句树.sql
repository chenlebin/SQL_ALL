---------select语法树------------
[
WITH CommonTableExpression (, CommonTableExpression)*]      -- 临时结果集
SELECT [ALL | DISTINCT] select_expr, select_expr, ...       -- 要查询的信息,distinct去重
  FROM table_reference                                      -- 来自哪个表？table_reference可以是表视图join结果和子查询结果
  [WHERE where_condition]                                   -- 条件查询
  [GROUP BY col_list]                                       -- 分组，使用分组之后select_expr只能是分组字段或者聚合函数中的字段
  [ORDER BY col_list]                                       -- 排序（全局排序）
  [CLUSTER BY col_list                                      -- 同一个字段进行分组+排序
    | [DISTRIBUTE BY col_list1] [SORT BY col_list2]           -- 不同字段分组 排序
  ]
 [LIMIT [offset,] rows]; -- 一个参数n时选中n行如果为两个参数n,m时，因为偏移量从0开始,所以选中n+1到m行