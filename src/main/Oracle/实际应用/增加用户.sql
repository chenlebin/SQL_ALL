insert into t_yhuser("UID", UNAME, ULX, UPWD)
values ('1008631', '老谢', ' 用户', '123456');
--参数：[1008631, 老谢, 用户, 123456]

select *
from T_YHUSER;

-- 1008604	陈乐斌	男	20010422	1585696186@qq.com
insert into T_YHXINXI
values ('1008631', '老谢', '男', '20010730', '123@qq.com');