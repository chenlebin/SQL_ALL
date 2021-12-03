--分页操作  这里我选择只显示第一页，一页有20行
select p.*
from (select rownum as r, t.*
      from (
               select state,
                      sum(cases)                                                AS cases, --计算确诊总人数
                      sum(deaths)                                               as deaths,--计算死亡总人数
                      CONCAT(ROUND(((sum(deaths) / sum(cases)) * 100), 2), '%') as dr     --计算死亡率
               from usa_zong
               where "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16'
               GROUP BY state
               ORDER BY cases desc) t) p
where r <= 20
  and r > 0;

-- CONCAT
--     (ROUND(((sum(deaths)/sum(cases))*100),2),'%')
--     as dr;

select *
FROM USA_ZONG;


-- 显示第一页  五行分一页
select p.*
from (select rownum r, t.*
      from (select e.* from emp e order by sal desc) t) p
where r <= 5
  and r > 0;


select *
from (
         select ROWNUM as r, t.*
         from (select * from emp order by SAL desc) t) p
where r <= 5
  and r > 0;
