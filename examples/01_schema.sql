-- Demo schema for db-assistant: airline boarding domain (PostgreSQL).
-- Run as a user with CREATE privilege on the target database.

CREATE SCHEMA IF NOT EXISTS demo;
SET search_path TO demo;

CREATE TABLE demo.airports (
    id      varchar(3)  NOT NULL,
    city    varchar(16),
    country varchar(2),
    active  boolean,
    CONSTRAINT airports_pkey PRIMARY KEY (id)
);

CREATE TABLE demo.plane_types (
    code        varchar(8)  NOT NULL,
    description varchar(64),
    CONSTRAINT plane_types_pkey PRIMARY KEY (code)
);

CREATE TABLE demo.statuses (
    id   varchar(12) NOT NULL,
    name varchar(64),
    CONSTRAINT statuses_pkey PRIMARY KEY (id)
);

CREATE TABLE demo.flights (
    id           varchar(6) NOT NULL,
    src          varchar(3),
    dst          varchar(3),
    takeoff_time time,
    CONSTRAINT flights_pkey PRIMARY KEY (id),
    CONSTRAINT flights_src_fk FOREIGN KEY (src) REFERENCES demo.airports (id),
    CONSTRAINT flights_dst_fk FOREIGN KEY (dst) REFERENCES demo.airports (id)
);

CREATE TABLE demo.planned_flights (
    flight        varchar(6) NOT NULL,
    flight_date   date       NOT NULL,
    boarding_time timestamp,
    takeoff_time  timestamp,
    delay_time    timestamp,
    status        varchar(12),
    plane         varchar(8),
    CONSTRAINT planned_flights_pkey PRIMARY KEY (flight, flight_date),
    CONSTRAINT planned_flights_flight_fk FOREIGN KEY (flight) REFERENCES demo.flights (id),
    CONSTRAINT planned_flights_plane_fk  FOREIGN KEY (plane)  REFERENCES demo.plane_types (code),
    CONSTRAINT planned_flights_status_fk FOREIGN KEY (status) REFERENCES demo.statuses (id)
);

CREATE INDEX idx_planned_flights_planes ON demo.planned_flights (plane);

CREATE TABLE demo.boardings (
    id                 integer    NOT NULL,
    flight             varchar(6) NOT NULL,
    flight_date        date       NOT NULL,
    first_name         varchar(24),
    last_name          varchar(24),
    id_num             varchar(16),
    reservation_number varchar(10),
    place              varchar(3),
    CONSTRAINT boardings_pkey PRIMARY KEY (id),
    CONSTRAINT boardings_flight_flight_date_place_key UNIQUE (flight, flight_date, place),
    CONSTRAINT boardings_planned_flight_fk
        FOREIGN KEY (flight, flight_date) REFERENCES demo.planned_flights (flight, flight_date),
    CONSTRAINT boardings_flight_fk FOREIGN KEY (flight) REFERENCES demo.flights (id)
);

CREATE INDEX boardings_reservation_number ON demo.boardings (reservation_number);
