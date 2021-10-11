CREATE TABLE black_list
(
    userid CHAR(1) PRIMARY KEY
);

CREATE TABLE user_ad_count
(
    dt      VARCHAR(255),
    userid  CHAR(1),
    adid    CHAR(1),
    `count` BIGINT,
    PRIMARY KEY (dt, userid, adid)
);