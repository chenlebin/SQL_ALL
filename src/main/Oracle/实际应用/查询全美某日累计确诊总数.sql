-- select state,sum(cases) AS cases,sum(deaths) as deaths,CONCAT(ROUND(((sum(deaths)/sum(cases))*100),2),'%') as dr from usa_zong 
-- where U_DATE="TO_DATE"('2020-03-26', 'yyyy-mm-dd') GROUP BY state ORDER BY cases desc;
-- 
CREATE VIEW usa5_16 as
select state,
       sum(cases)                                                as cases,
       sum(deaths)                                               as deaths,
       CONCAT(ROUND(((sum(deaths) / sum(cases)) * 100), 2), '%') as dr
from USA_ZONG
WHERE U_DATE = "TO_DATE"('2020-05-16', 'yyyy-mm-dd')
GROUP BY state
ORDER BY cases desc;

select "COUNT"(state) as xx
from usa5_16;

--drop view usa5_16;

select *
from usa5_16;

select 'usa' as state, "SUM"(cases) as cases, "SUM"(deaths) as deaths
FROM USA_ZONG
WHERE U_DATE = "TO_DATE"('2020-05-16', 'yyyy-mm-dd');

--查询全美
select 'usa' as state, "SUM"(cases) as cases, "SUM"(deaths) as deaths
FROM USA_ZONG
WHERE "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16';

select 'USA'                                                     as state,
       SUM(cases)                                                as cases,
       SUM(deaths)                                               as deaths,
       CONCAT(ROUND(((sum(deaths) / sum(cases)) * 100), 2), '%') as dr
FROM USA_ZONG
WHERE "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16';