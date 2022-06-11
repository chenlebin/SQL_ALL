--lateral view侧视图基本语法如下
--todo    select …… from tabelA lateral view UDTF(xxx) 表别名 as col1,col2,col3……;

use db_df2;

select a.team_name, b.year
from NBA_team_tmp a lateral view explode(a.year) b as year;

--根据年份倒序排序
select a.team_name, b.year
from NBA_team_tmp a lateral view explode(a.year) b as year
order by b.year desc;

--统计每个球队获取总冠军的次数 并且根据倒序排序
select a.team_name, count(*) over (partition by a.team_name) as nums, b.year
from NBA_team_tmp a lateral view explode(a.year) b as year
order by nums desc;

//因为要查询的字段还有year字段所以不能直接用group by
select a.team_name, count(*) as nums, b.year
from NBA_team_tmp a lateral view explode(a.year) b as year
group by team_name
order by nums desc;

-- 查看UDTF函数explode的用法
describe function extended explode;

select explode(`array`(11, 22, 33, 44, 55));

select explode(`map`("id", 10086, "name", "allen", "age", 18));