USE spark_poc;

CREATE TABLE property_info(
	prop_id text,
	chain_seq_desc text,
	address_1 text,
	address_2 text,
	city text,
	state text,
	zip int,
	phone_number text,
	gmt_offset int,
	currency text,
	latitude decimal,
	longitude decimal,
	corridor text,
	fitness_center boolean,
	high_speed boolean,
	meet_rooms_num int,
	num_rooms int,
	num_floors int,
	pool boolean,
	restaurant text,
	loc_code_desc text,
	census_region text,
	class_desc text,
	scale_chain text,
	scale_market text,
	scale_mkt_price text,
	scale_mkt_tract text,
	mkt_location_des text,
	PRIMARY KEY(prop_id)
);

USE spark_poc;

CREATE TABLE biz_leisure_score(
	hash_id int,
	biz_leisure_score decimal,
	PRIMARY KEY(hash_id)
);