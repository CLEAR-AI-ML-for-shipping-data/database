
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
	size_c FLOAT, 
	size_d FLOAT, 
	type_of_ship INTEGER, 
	type_of_cargo INTEGER, 
	type_of_ship_and_cargo INTEGER, 
	draught FLOAT, 
	ship_name VARCHAR(20), 
	owner VARCHAR, 
	valid_from DATE, 
	valid_to DATE, 
	PRIMARY KEY (ship_id), 
	UNIQUE (mmsi)
)

;


CREATE TABLE trajectories (
	trajectory_id SERIAL NOT NULL, 
	mmsi VARCHAR NOT NULL, 
	start_dt TIMESTAMP WITHOUT TIME ZONE, 
	end_dt TIMESTAMP WITHOUT TIME ZONE, 
	origin geometry(POINT,-1), 
	destination geometry(POINT,-1), 
	count INTEGER, 
	duration INTERVAL, 
	coordinates geometry(LINESTRING,-1), 
	timestamps TIMESTAMP WITHOUT TIME ZONE[], 
	speed_over_ground FLOAT[], 
	navigational_status INTEGER[], 
	PRIMARY KEY (trajectory_id, mmsi)
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
	latitude FLOAT, 
	longitude FLOAT, 
	geom geometry(POINT,-1), 
	navigational_status INTEGER, 
	speed_over_ground FLOAT, 
	course_over_ground FLOAT, 
	heading FLOAT, 
	country_ais VARCHAR, 
	destination VARCHAR, 
	rot FLOAT, 
	eot FLOAT, 
	PRIMARY KEY (timestamp, ship_id), 
	FOREIGN KEY(ship_id) REFERENCES ships (ship_id), 
	FOREIGN KEY(navigational_status) REFERENCES nav_status (id)
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
	count INTEGER, 
	duration INTERVAL, 
	ais_data geometry(LINESTRING,-1), 
	ais_timestamps TIMESTAMP WITHOUT TIME ZONE[], 
	PRIMARY KEY (voyage_id, ship_id), 
	FOREIGN KEY(ship_id) REFERENCES ships (ship_id)
)

;

