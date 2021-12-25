-- Q4. Plane Capacity Histogram

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q4 CASCADE;

CREATE TABLE q4 (
	airline CHAR(2),
	tail_number CHAR(5),
	very_low INT,
	low INT,
	fair INT,
	normal INT,
	high INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS intermediate_step CASCADE;
DROP VIEW IF EXISTS DepartedFlights CASCADE;
DROP VIEW IF EXISTS AllBookings CASCADE;
DROP VIEW IF EXISTS BookingsAndFlights CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW DepartedFlights as 
SELECT f.id as id, f.airline as airline, f.plane as plane, p.tail_number, capacity_economy+capacity_business+capacity_first as capacity
FROM departure d join flight f on d.flight_id = f.id join plane p on f.plane = p.tail_number;

CREATE VIEW AllBookings AS
SELECT flight_id, count(flight_id) 
FROM booking 
GROUP BY flight_id;

CREATE VIEW BookingsAndFlights as 
SELECT airline, tail_number, capacity, coalesce(count, 0) as numBookings 
FROM DepartedFlights left join AllBookings on DepartedFlights.id = AllBookings.flight_id; 

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q4
SELECT airline, tail_number, 
	sum(case 
		when numBookings*100/capacity >= 0 and numBookings*100/capacity < 20 THEN
		1
		ELSE
		0
		END) AS "very_low", 
	sum(case 
		when numBookings*100/capacity >= 20 and numBookings*100/capacity < 40 THEN
		1
		ELSE
		0
		END) AS "low", 
	sum(case 
		when numBookings*100/capacity >= 40 and numBookings*100/capacity < 60 THEN
		1
		ELSE
		0
		END) AS "fair", 
	sum(case 
		when numBookings*100/capacity >= 60 and numBookings*100/capacity < 80 THEN
		1
		ELSE
		0
		END) AS "normal",
	sum(case 
		when numBookings*100/capacity >= 80 THEN
		1
		ELSE
		0
		END) AS "high"
	FROM BookingsAndFlights
	GROUP BY airline, tail_number;
