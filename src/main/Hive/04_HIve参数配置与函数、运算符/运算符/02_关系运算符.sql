----------------Hive中关系运算符--------------------------
use db_df2;
--is null空值判断
select 1
from dual
where 'db_df2' is null;

--is not null 非空值判断
select 1
from dual
where 'db_df2' is not null;

--like比较： _表示任意单个字符 %表示任意数量字符
--否定比较： NOT A like B
select 1
from dual
where 'itcast' like 'it_';--false
select 1
from dual
where 'itcast' like 'it%';--true
select 1
from dual
where 'itcast' not like 'hadoo_';--true
select 1
from dual
where not 'itcast' like 'hadoo_';
--false

--rlike：确定字符串是否匹配正则表达式，是REGEXP_LIKE()的同义词。
--通配符的格式：^开头$结尾  \\d任意数字  \\s任意空格  +代表多个
select 1
from dual
where 'itcast' rlike '^i.*t$';
select 1
from dual
where '123456' rlike '^\\d+$'; --判断是否全为数字
select 1
from dual
where '123456aa' rlike '^\\d+$';

--regexp：功能与rlike相同 用于判断字符串是否匹配正则表达式
select 1
from dual
where 'itcast' regexp '^i.*t$';
