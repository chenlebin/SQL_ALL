--分页操作  这里我选择只显示第一页，一页有20行
select p.*
from (select rownum r, t.*
      from (
               select state,
                      sum(cases)                                                AS cases,
                      sum(deaths)                                               as deaths,
                      CONCAT(ROUND(((sum(deaths) / sum(cases)) * 100), 2), '%') as dr
               from usa_zong
               where "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16'
               GROUP BY state
               ORDER BY cases desc) t) p
where r <= 20
  and r > 0;

CONCAT
    (ROUND(((sum(deaths)/sum(cases))*100),2),'%')
    as dr;

select *
FROM USA_ZONG;


-- 显示第一页  五行分一页
select p.*
from (select rownum r, t.*
      from (select e.* from emp e order by sal desc) t) p
where r <= 5
  and r > 0;
