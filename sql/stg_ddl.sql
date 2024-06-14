-- ************************ stg ************************
--DROP SCHEMA IF EXISTS stg;
--CREATE SCHEMA stg;

DROP TABLE IF EXISTS stg.order_events;
CREATE TABLE stg.order_events (
	id serial4 NOT NULL PRIMARY KEY,
	object_id int4 NOT NULL,
	payload JSON NOT NULL,
	object_type varchar NOT NULL,
	sent_dttm timestamp NOT NULL,
	UNIQUE (object_id)
);