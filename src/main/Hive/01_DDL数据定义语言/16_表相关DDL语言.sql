/*表相关DDL语言
  time ：2021-11-15-15:25
  1 describe [formatted] 表名
  查看表的元数据信息

*/

--todo 清空表内数据，但是保留元数据
truncate table db_df2.nba_team_tmp;

--查询指定表的元数据信息
describe formatted db_df2.student;

use db_df2;

--1、更改表名
ALTER TABLE table_name
    RENAME TO new_table_name;
--2、更改表属性
ALTER TABLE table_name
    SET TBLPROPERTIES (property_name = property_value,... );
--更改表注释
ALTER TABLE student
    SET TBLPROPERTIES ('comment' = "new comment for student table");
--3、更改SerDe属性
ALTER TABLE table_name
    SET SERDE serde_class_name [WITH SERDEPROPERTIES (property_name = property_value,... )];
ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;
ALTER TABLE table_name
    SET SERDEPROPERTIES ('field.delim' = ',');
--移除SerDe属性
ALTER TABLE table_name [PARTITION partition_spec] UNSET SERDEPROPERTIES (property_name,... );

--4、更改表的文件存储格式 该操作仅更改表元数据。现有数据的任何转换都必须在Hive之外进行。
ALTER TABLE table_name
    SET FILEFORMAT file_format;
--5、更改表的存储位置路径
ALTER TABLE table_name
    SET LOCATION "new location";

--6、更改列名称/类型/位置/注释
CREATE TABLE test_change
(
    a int,
    b int,
    c int
);
// First change column a's name to a1.
ALTER TABLE test_change
    CHANGE a a1 INT;
// Next change column a1's name to a2, its data type to string, and put it after column b.
ALTER TABLE test_change
    CHANGE a1 a2 STRING AFTER b;
// The new table's structure is:  b int, a2 string, c int.
// Then change column c's name to c1, and put it as the first column.
ALTER TABLE test_change
    CHANGE c c1 INT FIRST;
// The new table's structure is:  c1 int, b int, a2 string.
// Add a comment to column a1
ALTER TABLE test_change
    CHANGE a1 a1 INT COMMENT 'this is column a1';

--7、添加/替换列
--使用ADD COLUMNS，您可以将新列添加到现有列的末尾但在分区列之前。
--REPLACE COLUMNS 将删除所有现有列，并添加新的列集。
ALTER TABLE table_name
    ADD| REPLACE COLUMNS (col_name data_type,...);










































































