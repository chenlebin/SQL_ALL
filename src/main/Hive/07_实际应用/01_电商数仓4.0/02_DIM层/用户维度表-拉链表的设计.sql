--在这里对用户维度表采取拉链表的设计，因为用户维度表的用户信息是可能发生变化，且变化的频率并不是非常大，
--数据量相对较小的。拉链表中想要获得最新的数据只需要加一个过滤条件就是结束时间=9999-99-99，
--想要获得某个时间点的历史数据则可以加上开始时间<=某个时间并且结束时间>=某个时间，
--这样设计的拉链表不仅能获取用户的最新数据也能随时获取用户任意时间的历史数据。
--创建拉链表
DROP TABLE IF EXISTS dim_user_info;
CREATE EXTERNAL TABLE dim_user_info
(
    `id`           STRING COMMENT '用户id',
    `login_name`   STRING COMMENT '用户名称',
    `nick_name`    STRING COMMENT '用户昵称',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dim/dim_user_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


--首日装载
insert overwrite table dim_user_info partition (dt = '9999-99-99')
select id,
       login_name,
       nick_name,
       md5(name),
       md5(phone_num),
       md5(email),
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       '2021-10-13',
       '9999-99-99'
from ods_user_info
where dt = '2021-10-13';


--每日装载
with--先进行新旧数据的full join
    tmp as
        (
            select old.id           old_id,
                   old.login_name   old_login_name,
                   old.nick_name    old_nick_name,
                   old.name         old_name,
                   old.phone_num    old_phone_num,
                   old.email        old_email,
                   old.user_level   old_user_level,
                   old.birthday     old_birthday,
                   old.gender       old_gender,
                   old.create_time  old_create_time,
                   old.operate_time old_operate_time,
                   old.start_date   old_start_date,
                   old.end_date     old_end_date,
                   new.id           new_id,
                   new.login_name   new_login_name,
                   new.nick_name    new_nick_name,
                   new.name         new_name,
                   new.phone_num    new_phone_num,
                   new.email        new_email,
                   new.user_level   new_user_level,
                   new.birthday     new_birthday,
                   new.gender       new_gender,
                   new.create_time  new_create_time,
                   new.operate_time new_operate_time,
                   new.start_date   new_start_date,
                   new.end_date     new_end_date
            from (
                     select id,
                            login_name,
                            nick_name,
                            name,
                            phone_num,
                            email,
                            user_level,
                            birthday,
                            gender,
                            create_time,
                            operate_time,
                            start_date,
                            end_date
                     from dim_user_info
                     where dt = '9999-99-99'
                 ) old
                     full outer join
                 (
                     select id,
                            login_name,
                            nick_name,
                            md5(name)      name,
                            md5(phone_num) phone_num,
                            md5(email)     email,
                            user_level,
                            birthday,
                            gender,
                            create_time,
                            operate_time,
                            '2021-10-14'   start_date,
                            '9999-99-99'   end_date
                     from ods_user_info
                     where dt = '2021-10-14'
                 ) new
                 on old.id = new.id
        )
--插入到用户维度表中采用动态分区，根据结束时间（增加一个字段和结束时间一样不过更名为dt）进行分区
insert
overwrite
table
dim_user_info
partition
(
dt
)
--取出新增数据和未变化数据和发生变化的新数据
select nvl(new_id, old_id),
       nvl(new_login_name, old_login_name),
       nvl(new_nick_name, old_nick_name),
       nvl(new_name, old_name),
       nvl(new_phone_num, old_phone_num),
       nvl(new_email, old_email),
       nvl(new_user_level, old_user_level),
       nvl(new_birthday, old_birthday),
       nvl(new_gender, old_gender),
       nvl(new_create_time, old_create_time),
       nvl(new_operate_time, old_operate_time),
       nvl(new_start_date, old_start_date),
       nvl(new_end_date, old_end_date),
       nvl(new_end_date, old_end_date) dt
from tmp
union all
--取出发生变化的旧数据修改其结束时间为 同步时间-1
select old_id,
       old_login_name,
       old_nick_name,
       old_name,
       old_phone_num,
       old_email,
       old_user_level,
       old_birthday,
       old_gender,
       old_create_time,
       old_operate_time,
       old_start_date,
       cast(date_add('2021-10-14', -1) as string),
       cast(date_add('2021-10-14', -1) as string) dt
from tmp
where new_id is not null
  and old_id is not null;


