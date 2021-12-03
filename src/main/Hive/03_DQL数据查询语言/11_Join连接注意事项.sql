------------------------------
--不等值连接
SELECT a.*
FROM a
         JOIN b ON (a.id = b.id)
SELECT a.*
FROM a
         JOIN b ON (a.id = b.id AND a.department = b.department)
SELECT a.*
FROM a
         LEFT OUTER JOIN b ON (a.id <> b.id)


SELECT a.val, b.val, c.val
FROM a
         JOIN b ON (a.key = b.key1)
         JOIN c ON (c.key = b.key2)


SELECT a.val, b.val, c.val
FROM a
         JOIN b ON (a.key = b.key1)
         JOIN c ON (c.key = b.key1)
--由于联接中仅涉及b的key1列，因此被转换为1个MR作业来执行
SELECT a.val, b.val, c.val
FROM a
         JOIN b ON (a.key = b.key1)
         JOIN c ON (c.key = b.key2)
--会转换为两个MR作业，因为在第一个连接条件中使用了b中的key1列，而在第二个连接条件中使用了b中的key2列。
-- 第一个map / reduce作业将a与b联接在一起，然后将结果与c联接到第二个map / reduce作业中。


SELECT a.val, b.val, c.val
FROM a
         JOIN b ON (a.key = b.key1)
         JOIN c ON (c.key = b.key1)
--由于联接中仅涉及b的key1列，因此被转换为1个MR作业来执行，并且表a和b的键的特定值的值被缓冲在reducer的内存中。然后，对于从c中检索的每一行，将使用缓冲的行来计算联接。
SELECT a.val, b.val, c.val
FROM a
         JOIN b ON (a.key = b.key1)
         JOIN c ON (c.key = b.key2)
--计算涉及两个MR作业。其中的第一个将a与b连接起来，并缓冲a的值，同时在reducer中流式传输b的值。
-- 在第二个MR作业中，将缓冲第一个连接的结果，同时将c的值通过reducer流式传输。


SELECT /*+ STREAMTABLE(a) */ a.val, b.val, c.val
FROM a
         JOIN b ON (a.key = b.key1)
         JOIN c ON (c.key = b.key1)
--a,b,c三个表都在一个MR作业中联接，并且表b和c的键的特定值的值被缓冲在reducer的内存中。
-- 然后，对于从a中检索到的每一行，将使用缓冲的行来计算联接。如果省略STREAMTABLE提示，则Hive将流式传输最右边的表。

SELECT /*+ MAPJOIN(b) */ a.key, a.value
FROM a
         JOIN b ON a.key = b.key
--不需要reducer。对于A的每个Mapper，B都会被完全读取。限制是不能执行FULL / RIGHT OUTER JOIN b。

