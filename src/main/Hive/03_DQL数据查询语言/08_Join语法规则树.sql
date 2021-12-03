-------------------Hive join语法规则树------------------------------
join_table:                                                                      -- 表连接
    table_reference [INNER] JOIN table_factor [join_condition]                   -- 内连接
  | table_reference {LEFT|RIGHT|FULL} [OUTER] JOIN table_reference join_condition-- (左/右/全)外连接
  | table_reference LEFT SEMI JOIN table_reference join_condition                -- 左半开连接（根据on条件返回左边满足条件的列不返回右边的）
  | table_reference CROSS JOIN table_reference [join_condition] (as of Hive 0.10)-- 交叉连接（产生笛卡尔积）

join_condition:                                                                  -- 连接条件
    ON expression
-------------------
--隐式联接表示法
SELECT *
FROM table1 t1,
     table2 t2,
     table3 t3
WHERE t1.id = t2.id
  AND t2.id = t3.id
  AND t1.zipcode = '02535';

--支持非等值连接
SELECT a.*
FROM a
         JOIN b ON (a.id = b.id)
SELECT a.*
FROM a
         JOIN b ON (a.id = b.id AND a.department = b.department)
SELECT a.*
FROM a
         LEFT OUTER JOIN b ON (a.id <> b.id)
-------------------
