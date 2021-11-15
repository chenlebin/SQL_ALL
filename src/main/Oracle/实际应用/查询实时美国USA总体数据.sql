UPDATE T_YHUSA
set UIDSUM=(select count("UID") from t_yhuser);

select uidsum
from T_YHUSA;

update T_YHUSA
set casessum=(
    select SUM(cases) as casessum
    FROM USA_ZONG
    WHERE "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16');

update T_YHUSA
set DEATHSSUM=(
    select SUM(deaths) as deathssum
    FROM USA_ZONG
    WHERE "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16');

SELECT *
from T_YHUSA;
