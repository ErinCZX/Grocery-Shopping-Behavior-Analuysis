DROP DATABASE IF EXISTS db_consumer_panel;
CREATE DATABASE db_consumer_panel;
USE db_consumer_panel;

DROP TABLE IF EXISTS raw_data_households;
DROP TABLE IF EXISTS raw_data_products;
DROP TABLE IF EXISTS raw_data_trips;
DROP TABLE IF EXISTS raw_data_purchases;

CREATE TABLE raw_data_households (
    hh_id                            INT(10) UNSIGNED NOT NULL,
    hh_race                          TINYINT(1) UNSIGNED,
    hh_is_latinx                     TINYINT(1) UNSIGNED,
    hh_zip_code                      MEDIUMINT(5) UNSIGNED,
    hh_income                        TINYINT(1) UNSIGNED,
    hh_state                         CHAR(10),
    hh_size                          TINYINT(1) UNSIGNED,
    hh_residence_type                TINYINT(1) UNSIGNED
)  ENGINE = INNODB;

CREATE TABLE raw_data_products (
    brand_at_prod_id                 CHAR(50),
    department_at_prod_id            CHAR(50),
    prod_id                          BIGINT(16) UNSIGNED NOT NULL,
    group_at_prod_id                 CHAR(50),
    module_at_prod_id                CHAR(100),
    amount_at_prod_id                FLOAT,
    units_at_prod_id                 CHAR(50)
)  ENGINE = INNODB;

CREATE TABLE raw_data_trips (
    hh_id                            INT(10) UNSIGNED NOT NULL,
    TC_date                          DATE,
    TC_retailer_code                 SMALLINT(5) UNSIGNED,
    TC_retailer_code_store_code      MEDIUMINT(10) UNSIGNED,
    TC_retailer_code_store_zip3      FLOAT,
    TC_total_spent                   MEDIUMINT(10) UNSIGNED,
    TC_id                            INT(10) UNSIGNED NOT NULL
)  ENGINE = INNODB;

CREATE TABLE raw_data_purchases (
    TC_id                            INT(10) UNSIGNED NOT NULL,
    quantity_at_TC_prod_id           MEDIUMINT(5) UNSIGNED,
    total_price_paid_at_TC_prod_id   FLOAT,
    coupon_value_at_TC_prod_id       FLOAT,
    deal_flag_at_TC_prod_id          TINYINT(1) UNSIGNED,
    prod_id                          BIGINT(16) UNSIGNED NOT NULL
)  ENGINE = INNODB;

DROP TABLE IF EXISTS purchases;
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS households;

CREATE TABLE households AS
    SELECT DISTINCT
        hh_id,
        hh_race,
        hh_is_latinx,
        hh_income,
        hh_size,
        hh_zip_code,
        hh_state,
        hh_residence_type
    FROM raw_data_households;
    
ALTER TABLE households ADD PRIMARY KEY (hh_id);

CREATE TABLE products AS
    WITH
    distinct_table AS (
        SELECT DISTINCT
            prod_id,
            brand_at_prod_id,
            department_at_prod_id,
            group_at_prod_id,
            module_at_prod_id,
            amount_at_prod_id,
            units_at_prod_id
        FROM raw_data_products)
    SELECT s.*
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY prod_id ORDER BY brand_at_prod_id) AS group_index
        FROM distinct_table) AS s
    WHERE s.group_index = 1;

ALTER TABLE products ADD PRIMARY KEY (prod_id);

CREATE TABLE trips AS
    SELECT
        TC_id,
        hh_id,
        TC_date,
        TC_retailer_code,
        TC_retailer_code_store_code,
        TC_retailer_code_store_zip3,
        TC_total_spent
    FROM raw_data_trips;

ALTER TABLE trips ADD PRIMARY KEY (TC_id);
ALTER TABLE trips ADD CONSTRAINT FK_hh_id FOREIGN KEY (hh_id) REFERENCES Households(hh_id);

CREATE TABLE purchases AS
    SELECT DISTINCT
        TC_id,
        prod_id,
        quantity_at_TC_prod_id,
        total_price_paid_at_TC_prod_id,
        coupon_value_at_TC_prod_id,
        deal_flag_at_TC_prod_id
    FROM raw_data_purchases;

ALTER TABLE purchases ADD CONSTRAINT FK_TC_id FOREIGN KEY (TC_id) REFERENCES trips(TC_id);
ALTER TABLE purchases ADD CONSTRAINT FK_prod_id FOREIGN KEY (prod_id) REFERENCES products(prod_id);
ALTER TABLE purchases ADD INDEX index_prod_id(prod_id);
ALTER TABLE purchases ADD INDEX index_tc_id(TC_id);