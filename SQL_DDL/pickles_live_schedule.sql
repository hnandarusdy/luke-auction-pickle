-- auto-generated definition
create table pickles_live_schedule
(
    id                   int auto_increment primary key,
    category             varchar(255)                        null,
    title                varchar(500)                        null,
    location             varchar(255)                        null,
    status               varchar(100)                        null,
    sale_info_url        varchar(1000)                       null,
    auction_registration varchar(1000)                       null,
    sale_title           varchar(500)                        null,
    sale_date            varchar(255)                        null,
    sale_occurs          text                                null,
    created_at           timestamp default CURRENT_TIMESTAMP null,
    updated_at           timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    auction_type         varchar(50)                         null,
    start_sale_date      varchar(30)                         null,
    end_sale_date        varchar(30)                         null
);

