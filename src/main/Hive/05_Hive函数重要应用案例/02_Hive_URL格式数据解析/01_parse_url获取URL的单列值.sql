--------------------------------URL解析--------------------------------------------------------
--todo parse_url函数是Hive中提供的最基本的url解析函数，可以根据指定的参数，
--     从URL解析出对应的参数值进行返回，函数为普通的一对一函数类型UDF函数。

-- parse_url(url, partToExtract[, key]) - extracts a part from a URL
-- todo Parts: HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, USERINFO key


SELECT parse_url('http://facebook.com/path/p1.php?id=10086', 'PATH');

SELECT parse_url('http://facebook.com/path/p1.php?id=10086&name=allen', 'QUERY');

SELECT parse_url('http://facebook.com/path/p1.php?id=10086&name=allen', 'HOST');

