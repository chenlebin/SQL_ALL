-- 			yhs.setUname(u_name1);
-- 			yhs.setUpwd(u_pwd1);
-- 			yhs.setUxb(u_xb1);
-- 			yhs.setUsf(u_sf);
-- 			yhs.setUyx(u_yx1);

-- 执行更新操作
update T_YHUSER
set uname='乐斌',
    UPWD='12345678'
where "UID" = 1008604;
update T_YHXINXI
set uname='乐斌',
    UXB='女',
    USF='20010423',
    UYX='158569618@qq.com'
where "UID" = 1008604;

-- 查询是否更新成功
select *
from T_YHUSER
         INNER JOIN T_YHXINXI on T_YHUSER."UID" = T_YHXINXI."UID"
where T_YHUSER."UID" = 1008604;