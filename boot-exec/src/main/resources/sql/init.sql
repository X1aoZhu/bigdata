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

create table sys_product
(
    id          bigint       not null
        primary key,
    name        varchar(256) null,
    price       decimal      null,
    create_time timestamp    null,
    update_time timestamp    null
);