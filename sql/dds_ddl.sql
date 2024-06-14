-- ************************ dds ************************
-- ************************ хабы ************************
--DROP SCHEMA IF EXISTS dds;
--CREATE SCHEMA dds;

DROP TABLE IF EXISTS dds.h_user;
CREATE TABLE dds.h_user (
	h_user_pk UUID NOT NULL PRIMARY KEY,
	user_id VARCHAR NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dds.h_product;
CREATE TABLE dds.h_product (
	h_product_pk UUID NOT NULL PRIMARY KEY,
	product_id VARCHAR NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dds.h_category;
CREATE TABLE dds.h_category (
	h_category_pk UUID NOT NULL PRIMARY KEY,
	category_name VARCHAR NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dds.h_restaurant;
CREATE TABLE dds.h_restaurant (
	h_restaurant_pk UUID NOT NULL PRIMARY KEY,
	restaurant_id VARCHAR NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dds.h_order;
CREATE TABLE dds.h_order (
	h_order_pk UUID NOT NULL PRIMARY KEY,
	order_id int4 NOT NULL,
	order_dt timestamp NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

-- ************************ линки ************************
DROP TABLE IF EXISTS dds.l_order_product;
CREATE TABLE dds.l_order_product (
	hk_order_product_pk UUID NOT NULL PRIMARY KEY,
	h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
	h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dds.l_product_restaurant;
CREATE TABLE dds.l_product_restaurant (
	hk_product_restaurant_pk UUID NOT NULL PRIMARY KEY,
	h_restaurant_pk UUID NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk),
	h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dds.l_product_category;
CREATE TABLE dds.l_product_category (
	hk_product_category_pk UUID NOT NULL PRIMARY KEY,
	h_category_pk UUID NOT NULL REFERENCES dds.h_category(h_category_pk),
	h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dds.l_order_user;
CREATE TABLE dds.l_order_user (
	hk_order_user_pk UUID NOT NULL PRIMARY KEY,
	h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
	h_user_pk UUID NOT NULL REFERENCES dds.h_user(h_user_pk),
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL
);

-- ************************ саттелиты ************************
DROP TABLE IF EXISTS dds.s_user_names;
CREATE TABLE dds.s_user_names (
	h_user_pk UUID NOT NULL REFERENCES dds.h_user(h_user_pk),
	username VARCHAR NOT NULL,
	userlogin VARCHAR NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_user_names_hashdiff UUID NOT NULL,
	CONSTRAINT s_user_names_pkey PRIMARY KEY (h_user_pk, load_dt)
);

DROP TABLE IF EXISTS dds.s_product_names;
CREATE TABLE dds.s_product_names (
	h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
	name VARCHAR NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_product_names_hashdiff UUID NOT NULL,
	CONSTRAINT s_product_names_pkey PRIMARY KEY (h_product_pk, load_dt)
);

DROP TABLE IF EXISTS dds.s_restaurant_names;
CREATE TABLE dds.s_restaurant_names (
	h_restaurant_pk UUID NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk),
	name VARCHAR NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_restaurant_names_hashdiff UUID NOT NULL,
	CONSTRAINT s_restaurant_names_pkey PRIMARY KEY (h_restaurant_pk, load_dt)
);

DROP TABLE IF EXISTS dds.s_order_cost;
CREATE TABLE dds.s_order_cost (
	h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
	payment decimal(19, 5) NOT NULL,
	COST decimal(19, 5) NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_order_cost_hashdiff UUID NOT NULL,
	CONSTRAINT s_order_cost_pkey PRIMARY KEY (h_order_pk, load_dt)
);

DROP TABLE IF EXISTS dds.s_order_status;
CREATE TABLE dds.s_order_status (
	h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
	status VARCHAR NOT NULL,
	load_dt timestamp NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_order_status_hashdiff UUID NOT NULL,
	CONSTRAINT s_order_status_pkey PRIMARY KEY (h_order_pk, load_dt)
);