----------- Date Functions 日期函数 -----------------
desc function extended lpad;
SELECT lpad('hi', 5, '1');
--获取当前日期: current_date（yyyy-MM-dd）
select current_date();
--获取当前时间戳: current_timestamp
--同一查询中对current_timestamp的所有调用均返回相同的值。
select current_timestamp();

--todo 时间处理
--获取当前UNIX时间戳函数: unix_timestamp
select unix_timestamp();
--日期转UNIX时间戳函数: unix_timestamp

select unix_timestamp("2011-12-07 13:01:03");

--默认为：'yyyy-MM-dd HH:mm:ss'
--指定格式日期转UNIX时间戳函数: unix_timestamp
select unix_timestamp('20111207 13:01:03', 'yyyyMMdd HH:mm:ss');

select (unix_timestamp('2021-12-23', 'yyyy-MM-dd') - unix_timestamp('2021-12-22', 'yyyy-MM-dd')) / (60 * 60 * 24);
--UNIX时间戳转日期函数: from_unixtime
select from_unixtime(1618238391);
select from_unixtime(0, 'yyyy-MM-dd HH:mm:ss');
--日期比较函数: datediff  日期格式要求'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'
select datediff('2012-6-08', '2012-05-09');
--日期增加函数: date_add
select date_add('2012-02-28', 10);
--日期减少函数: date_sub
select date_sub('2012-01-1', 10);


--抽取日期函数: to_date
select to_date('2009-07-30 04:17:52');
--日期转年函数: year
select year('2009-07-30 04:17:52');
--日期转月函数: month
select month('2009-07-30 04:17:52');
--日期转天函数: day
select day('2009-07-30 04:17:52');
--日期转小时函数: hour
select hour('2009-07-30 04:17:52');
--日期转分钟函数: minute
select minute('2009-07-30 04:17:52');
--日期转秒函数: second
select second('2009-07-30 04:17:52');
--日期转周函数: weekofyear 返回指定日期所示年份第几周
select weekofyear('2009-07-30 04:17:52');
