-------------Mathematical Functions 数学函数-------------
--取整函数: round  返回double类型的整数值部分 （遵循四舍五入）
select round(3.1415926);
--指定精度取整函数: round(double a, int d) 返回指定精度d的double类型
--4是小数的精度不是总长度的精度
select round(3.1415926, 4);
--向下取整函数: floor
select floor(3.1415926);
select floor(-3.1415926);
--向上取整函数: ceil
select ceil(3.1415926);
select ceil(-3.1415926);
--取随机数函数: rand 每次执行都不一样 返回一个0到1范围内的随机数
select rand();
--todo 指定种子取随机数函数: rand(int seed) 得到一个稳定的随机数序列
select rand(3);


--二进制函数:  bin(BIGINT a)
select bin(18);
--进制转换函数: conv(BIGINT num, int from_base, int to_base)
--要转换的参数，原来的类型，转换之后的类型
select conv(17, 10, 16);
--绝对值函数: abs
select abs(-3.9);
