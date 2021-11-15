update T_YHUSA
set UIDSUM=5
UPDATE T_YHUSA
set UIDSUM=(select count("UID") from t_yhuser);

select count("UID")
from t_yhuser;

select *
FROM T_YHUSA;

select uidsum
from T_YHUSA;