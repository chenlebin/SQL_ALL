show functions;

desc function extended concat;

desc function extended concat;

show tables;

describe function extended get_json_object;

------------String Functions 字符串函数------------
select concat("angela", "baby");

--带分隔符字符串连接函数：concat_ws(separator, [string | array(string)]+)
select concat_ws('.', 'www', array('itcast', 'cn'));

--字符串截取函数：substr(str, pos[, len]) 或者  substring(str, pos[, len])pos是从1开始的索引，如果为负数则倒着数
select substr("angelababy", -2); --

select substr("angelababy", 2, 2);

--正则表达式替换函数：regexp_replace(str, regexp, rep)
select regexp_replace('100-200', '(\\d+)', 'num');

--正则表达式解析函数：regexp_extract(str, regexp[, idx]) 提取正则匹配到的指定组内容
select regexp_extract('100-200', '(\\d+)-(\\d+)', 2);

--URL解析函数：parse_url 注意要想一次解析出多个 可以使用parse_url_tuple这个UDTF函数
select parse_url('http://www.itcast.cn/path/p1.php?query=1', 'HOST');

--分割字符串函数: split(str, regex)  \\s+代表任意长度的空白字符
select split('apache hive', '\\s+');

select split('apache,hive,3', ',');


--json解析函数：get_json_object(json_txt, path)
--$表示json对象
select get_json_object(
               '[{"website":"www.itcast.cn","name":"allenwoon"}, {"website":"cloud.itcast.com","name":"carbondata 中文文档"}]',
               '$.[1].website');


--字符串长度函数：length(str | binary)
select length("angelababy");

--字符串反转函数：reverse
select reverse("angelababy");

--字符串连接函数：concat(str1, str2, ... strN)
--字符串转大写函数：upper,ucase
select upper("angelababy");
select ucase("angelababy");

--字符串转小写函数：lower,lcase
select lower("ANGELABABY");
select lcase("ANGELABABY");

--去空格函数：trim 去除左右两边的空格
select trim(" angelababy ");

--左边去空格函数：ltrim
select ltrim(" angelababy ");

--右边去空格函数：rtrim
select rtrim(" angelababy ");

--空格字符串函数：space(n) 返回指定个数空格
select concat(space(4), "xx");

--重复字符串函数：repeat(str, n) 重复str字符串n次
select repeat("angela", 2);

--提取首字符的ascii函数：ascii
select ascii("angela");
--a对应ASCII 97

--左补足函数：lpad
select lpad('hi', 5, '??'); --???hi
select lpad('hi', 1, '??');
--h

--右补足函数：rpad
select rpad('hi', 10, '123');

--集合查找函数: find_in_set(str,str_array),查找在集合中出现的第一个位置
select find_in_set('a', '1,b,ab,c,def,a');//6 注意查找的是字符，如果字符串包含整个字符是不正确的