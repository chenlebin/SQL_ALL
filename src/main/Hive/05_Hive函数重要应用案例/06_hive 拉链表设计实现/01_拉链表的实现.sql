--------------------------------hive 拉链表设计实现-------------------------------------------------------

--拉链表专门用于解决在数据仓库中数据发生变化如何实现数据存储的问题。
--拉链表的设计是将更新的数据进行状态记录，没有发生更新的数据不进行状态存储，
--v用于存储所有数据在不同时间上的所有状态，通过时间进行标记每个状态的生命周期，
--查询时，根据需求可以获取指定时间范围状态的数据，默认用9999-12-31等最大值来表示最新状态。。

--建表加载数据
--todo 1、创建拉链表
create table dw_zipper
(
    userid    string,
    phone     string,
    nick      string,
    gender    int,
    addr      string,
    starttime string,
    endtime   string
) row format delimited fields terminated by '\t';

--加载模拟数据
load data local inpath '/root/hivedata/zipper.txt' into table dw_zipper;
--查询
select userid, nick, addr, starttime, endtime
from dw_zipper;


--todo 2、创建ods层增量表 加载数据
create table ods_zipper_update
(
    userid    string,
    phone     string,
    nick      string,
    gender    int,
    addr      string,
    starttime string,
    endtime   string
) row format delimited fields terminated by '\t';


select *
from ods_zipper_update;

--合并数据
--todo 3、创建临时表
drop table tmp_zipper;
create table tmp_zipper
(
    userid    string,
    phone     string,
    nick      string,
    gender    int,
    addr      string,
    starttime string,
    endtime   string
) row format delimited fields terminated by '\t';

select *
from tmp_zipper;

--todo 4、合并拉链表与增量表
--update数据(b表数据)
--008	186xxxx1241	laoba	1	sh	2021-01-02	9999-12-31
--011	186xxxx1244	laoshi	1	jx	2021-01-02	9999-12-31
--012	186xxxx1245	laoshi	0	zj	2021-01-02	9999-12-31

--增加新增数据
load data local inpath '/root/hivedata/update1.txt' overwrite into table ods_zipper_update;

select *
from ods_zipper_update;

insert overwrite table tmp_zipper
select userid,
       phone,
       nick,
       gender,
       addr,
       starttime,
       endtime
--todo 新增的新数据直接Union到tmp_zipper表中
from ods_zipper_update
union all
--查询原来拉链表的所有数据，并将这次需要更新的数据的endTime更改为更新值的startTime
select a.userid,
       a.phone,
       a.nick,
       a.gender,
       a.addr,
       a.starttime,
       --todo 对拉链表中的发生改变的数据进行更新操作，未发生改变的数据保持不变

       -- 如果这条数据没有更新或者这条数据不是要更改的数据，就保留原来的值，否则就改为新数据的开始时间-1
       -- b.userid为null时说明在右表中不存在这一条数据是拉链表中未改变的数据，则数据不变，或者a.ending<'9999-12-31'
       -- 则说明这条数据就是历史数据不需要修改
       -- 只有当发现是发生改变的数据:（b.userid不为null,代表右表中存在,代表是右表的新增数据）并且a.endtime='9999-12-31'
       -- (说明这条数据不是历史数据是需要更新的)时才说明这条数据是对拉链表的数据的更新操作
       -- 对老数据进行更新，a.endtime为b.starttime-1,使用date_sub(date,number)日期减少函数。
       if(b.userid is null or a.endtime < '9999-12-31', a.endtime, date_sub(b.starttime, 1)) as endtime
from dw_zipper a
         left join ods_zipper_update b
                   on a.userid = b.userid;

desc function extended date_sub;


--todo 5、覆盖拉链表
insert overwrite table dw_zipper
select *
from tmp_zipper;

select *
from tmp_zipper;

select *
from dw_zipper;