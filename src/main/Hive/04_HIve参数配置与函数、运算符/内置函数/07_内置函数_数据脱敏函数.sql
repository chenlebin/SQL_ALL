----Data Masking Functions 数据脱敏函数------------
--todo 掩码处理
--mask
--将查询回的数据，大写字母转换为X，小写字母转换为x，数字转换为n。
select mask("abc123DEF");--xxxnnnXXX
select mask("abc123DEF", '-', '.', '^');
--自定义替换的字母

--mask_first_n(string str[, int n]
--对前n个进行脱敏替换
select mask_first_n("abc123DEF", 4);

--对后n个进行脱敏替换
--mask_last_n(string str[, int n])
select mask_last_n("abc123DEF", 4);

--mask_show_first_n(string str[, int n])
--除了前n个字符，其余进行掩码处理
select mask_show_first_n("abc123DEF", 4);

--除了后n个字符，其余进行掩码处理
--mask_show_last_n(string str[, int n])
select mask_show_last_n("abc123DEF", 4);

--mask_hash(string|char|varchar str)
--返回字符串的hash编码。
select mask_hash("abc123DEF");
