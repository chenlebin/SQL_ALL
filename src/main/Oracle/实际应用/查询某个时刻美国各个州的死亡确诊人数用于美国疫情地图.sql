select "TO_CHAR"(U_DATE, 'MM-dd') as U_DATE, STATE, sum(cases) as cases, sum(deaths) as deaths
FROM USA_ZONG
where "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16'
  and state in
      (select state
       from usa_zong
       where "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16')
GROUP BY U_DATE, state
ORDER BY STATE;

select *
from USA_ZONG;

--  子查询结果
select state
from usa_zong
where "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16';
