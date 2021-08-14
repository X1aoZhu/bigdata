create table if not exists `sys_order`
(
    id           bigint primary key,
    total_amount decimal,
    status       int,
    create_time  date,
    update_time  date
);

create table if not exists order_item
(
    id                bigint primary key,
    order_it          bigint,
    product_id        bigint,
    product_count     int,
    order_item_amount decimal,
    create_time       date,
    update_time       date
);

create table if not exists product
(
    id          bigint primary key,
    name        varchar(32),
    price       decimal,
    create_time date,
    update_time date
);

insert into flink_test1.sys_order(id, total_amount, status, create_time, update_time)
VALUES (1, 100, 1, now(), now());