--单个lateral view使用
use db_df2;
select a.id    as id,
       b.host  as host,
       b.path  as path,
       b.query as query
from tb_url a lateral view parse_url_tuple(url, "HOST", "PATH", "QUERY") b as host, path, query;

--todo 可以多个lateral view一起使用
select a.id       as id,
       b.host     as host,
       b.path     as path,
       c.protocol as protocol,
       c.query    as query
from tb_url a
         lateral view parse_url_tuple(url, "HOST", "PATH") b as host, path
         lateral view parse_url_tuple(url, "PROTOCOL", "QUERY") c as protocol, query;

---Outer Lateral View
--如果UDTF不产生数据时，这时侧视图与原表关联的结果将为空
select id,
       url,
       col1
from tb_url
         lateral view explode(array()) et as col1;


--如果加上outer关键字以后，就会保留原表数据，类似于full outer join
select id,
       url,
       col1
from tb_url
         lateral view outer explode(array()) et as col1;



select *
from tb_url;