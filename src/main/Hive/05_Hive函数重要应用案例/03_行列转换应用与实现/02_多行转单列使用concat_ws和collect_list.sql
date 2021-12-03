--2、多行转单列
use db_df2;
select *
from row2col1;
--concat(参数1,参数2,......),当参数中一个存在null值时则返回结果为空
select concat("it", "cast", "And", "heima");
select concat("it", "cast", "And", null);

--concat_ws(分隔符,参数1,参数2,......),即使参数中存在null值返回值依然不为空
select concat_ws("-", "itcast", "And", "heima");
select concat_ws("-", "itcast", "And", null);

--collect_list(列名) 将一列中的多行合并在一起，不进行去重 ALL
select collect_list(col1)
from row2col1;

--collect_set(列名)  将一列中的多行合并在一起，进行去重   Distance
select collect_set(col1)
from row2col1;


--建表
create table row2col2
(
    col1 string,
    col2 string,
    col3 int
) row format delimited fields terminated by '\t';

--加载数据到表中
load data local inpath '/root/hivedata/r2c2.txt' into table row2col2;

select *
from row2col2;

describe function extended concat_ws;

--最终SQL实现
select col1,
       col2,
       concat_ws(',', collect_list(cast(col3 as string))) as col3
from row2col2
group by col1, col2;
