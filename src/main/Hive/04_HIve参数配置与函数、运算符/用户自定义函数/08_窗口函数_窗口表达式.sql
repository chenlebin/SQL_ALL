---窗口表达式
use db_df2;
select cookieid,
       createtime,
       pv,
       sum(pv) over (partition by cookieid order by createtime) as pv1 --默认从第一行到当前行
from website_pv_info;
--第一行到当前行
select cookieid,
       createtime,
       pv,
       sum(pv) over (partition by cookieid order by createtime rows between unbounded preceding and current row) as pv2
from website_pv_info;

--向前3行至当前行
select cookieid,
       createtime,
       pv,
       sum(pv) over (partition by cookieid order by createtime rows between 3 preceding and current row) as pv4
from website_pv_info;

--向前3行 向后1行
select cookieid,
       createtime,
       pv,
       sum(pv) over (partition by cookieid order by createtime rows between 3 preceding and 1 following) as pv5
from website_pv_info;

--当前行至最后一行
select cookieid,
       createtime,
       pv,
       sum(pv) over (partition by cookieid order by createtime rows between current row and unbounded following) as pv6
from website_pv_info;

--第一行到最后一行 也就是分组内的所有行,
--相当于order by的连续累加聚合是无效的还是分组内整体聚合但是排序效果依然在
select cookieid,
       createtime,
       pv,
       sum(pv)
           over (partition by cookieid order by createtime rows between unbounded preceding and unbounded following) as pv7
from website_pv_info;
