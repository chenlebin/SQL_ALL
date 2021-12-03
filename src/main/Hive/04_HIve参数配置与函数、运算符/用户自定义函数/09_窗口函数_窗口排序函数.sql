-----窗口排序函数
-- todo row_number、rank、dense_rank
-- todo 适合用于TopN的问题
-- row_number：在每个分组中，为每行分配一个从1开始的唯一序列号，递增，不考虑重复
-- rank: 在每个分组中，为每行分配一个从1开始的序列号，考虑重复，挤占后续位置
-- dense_rank: 在每个分组中，为每行分配一个从1开始的序列号，考虑重复，不挤占后续位置
use db_df2;

SELECT cookieid,
       createtime,
       pv,
       RANK() OVER (PARTITION BY cookieid ORDER BY pv desc)       AS rn1,
       DENSE_RANK() OVER (PARTITION BY cookieid ORDER BY pv desc) AS rn2,
       ROW_NUMBER() OVER (PARTITION BY cookieid ORDER BY pv DESC) AS rn3
FROM website_pv_info
WHERE cookieid = 'cookie1';

--需求：找出每个用户访问pv最多的Top3 重复并列的不考虑
SELECT *
from (SELECT cookieid,
             createtime,
             pv,
             ROW_NUMBER() OVER (PARTITION BY cookieid ORDER BY pv DESC) AS seq
      FROM website_pv_info) tmp
where tmp.seq < 4;


-- todo ntile
-- todo 适用于取数据的m/n取数据的n分之m，n代表分桶数
-- 将每个分组内的数据分为指定的若干个桶里（分为若干个部分），并且为每一个桶分配一个桶编号。
-- 如果不能平均分配，则优先分配较小编号的桶，并且各个桶中能放的行数最多相差1。例如有7行数据则分成3组之后为3,2,2
-- 有时会有这样的需求:如果数据排序后分为三部分，业务人员只关心其中的一部分，如何将这中间的三分之一数据拿出来呢?NTILE函数即可以满足。

-- 把每个分组内的数据分为3桶
SELECT cookieid,
       createtime,
       pv,
       NTILE(3) OVER (PARTITION BY cookieid ORDER BY createtime) AS rn2
FROM website_pv_info
ORDER BY cookieid, createtime;

--需求：统计每个用户pv数最多的前3分之1天。
--理解：将数据根据cookieid分 根据pv倒序排序 排序之后分为3个部分 取第一部分
SELECT *
from (SELECT cookieid,
             createtime,
             pv,
             NTILE(3) OVER (PARTITION BY cookieid ORDER BY pv DESC) AS rn
      FROM website_pv_info) tmp
where rn = 1;
