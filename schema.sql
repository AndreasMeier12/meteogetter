CREATE TABLE station
(
    id        INTEGER PRIMARY KEY,
    name      TEXT UNIQUE,
    abbr      TEXT UNIQUE,
    latitude  REAL,
    longitude REAL,
    altitude  REAL
);

CREATE TABLE humidity
(
    station_id  INTEGER,
    measurement REAL,
    timestamp   INTEGER,
    FOREIGN KEY (station_id) REFERENCES satation(id)
);

CREATE TABLE temperature
(
    station_id  INTEGER,
    measurement REAL,
    timestamp   INTEGER,
    FOREIGN KEY (station_id) REFERENCES satation(id)
);