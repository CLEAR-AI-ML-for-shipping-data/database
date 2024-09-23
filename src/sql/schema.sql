
CREATE TABLE nav_status (
	id SERIAL NOT NULL, 
	code VARCHAR, 
	description VARCHAR, 
	PRIMARY KEY (id)
)

;


CREATE TABLE ships (
	ship_id BIGSERIAL NOT NULL, 
	mmsi VARCHAR(20), 
	imo VARCHAR(20), 
	size_a FLOAT, 
	size_b FLOAT, 
	ship_type INTEGER, 
	cargo_type INTEGER, 
	ship_and_cargo_type INTEGER, 
	draught FLOAT, 
	ship_name VARCHAR(20), 
	owner VARCHAR, 
	valid_from DATE, 
	valid_to DATE, 
	PRIMARY KEY (ship_id), 
	UNIQUE (mmsi)
)

;


CREATE TABLE voyage_models (
	id SERIAL NOT NULL, 
	comment VARCHAR, 
	script VARCHAR, 
	PRIMARY KEY (id), 
	UNIQUE (comment)
)

;


CREATE TABLE "Complete_Voyages" (
	voyage_id SERIAL NOT NULL, 
	ship_id INTEGER NOT NULL, 
	voyage_model_id INTEGER, 
	start_dt TIMESTAMP WITHOUT TIME ZONE, 
	end_dt TIMESTAMP WITHOUT TIME ZONE, 
	origin geometry(POINT,-1), 
	destination geometry(POINT,-1), 
	origin_port VARCHAR, 
	destination_port VARCHAR, 
	origin_port_distance FLOAT, 
	destination_port_distance FLOAT, 
	ais_data geometry(LINESTRING,-1), 
	PRIMARY KEY (voyage_id, ship_id), 
	FOREIGN KEY(ship_id) REFERENCES ships (ship_id)
)

;


CREATE TABLE ais_data (
	timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	ship_id INTEGER NOT NULL, 
	lat FLOAT, 
	lon FLOAT, 
	nav_status VARCHAR(20), 
	speed FLOAT, 
	course FLOAT, 
	heading FLOAT, 
	destination VARCHAR, 
	rot FLOAT, 
	eot FLOAT, 
	PRIMARY KEY (timestamp, ship_id), 
	FOREIGN KEY(ship_id) REFERENCES ships (ship_id)
)

;


CREATE TABLE voyage_segments (
	voyage_id SERIAL NOT NULL, 
	ship_id INTEGER NOT NULL, 
	voyage_model_id INTEGER, 
	start_dt TIMESTAMP WITHOUT TIME ZONE, 
	end_dt TIMESTAMP WITHOUT TIME ZONE, 
	origin geometry(POINT,-1), 
	destination geometry(POINT,-1), 
	origin_port VARCHAR, 
	destination_port VARCHAR, 
	origin_port_distance FLOAT, 
	destination_port_distance FLOAT, 
	ais_data geometry(LINESTRING,-1), 
	PRIMARY KEY (voyage_id, ship_id), 
	FOREIGN KEY(ship_id) REFERENCES ships (ship_id)
)

;

