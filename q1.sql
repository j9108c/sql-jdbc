-- Q1. Airlines

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q1 CASCADE;

CREATE TABLE q1 (
    pass_id INT,
    name VARCHAR(100),
    airlines INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.

DROP VIEW IF EXISTS step1 CASCADE;
drop view if exists step2 cascade;
drop view if exists step3 cascade;
drop view if exists step4 cascade;

-- Define views for your intermediate steps here:

create view step1 as
select Passenger.id as pass_id,
    Passenger.firstname||' '||Passenger.surname as name,
    count(distinct Flight.airline) as airlines
from Passenger
    join Booking on Passenger.id = Booking.pass_id
    join Flight on Booking.flight_id = Flight.id
    join Departure on Flight.id = Departure.flight_id
group by Passenger.id
order by Passenger.id;

create view step2 as
select Passenger.id as pass_id,
    Passenger.firstname||' '||Passenger.surname as name,
    0 as airlines
from Passenger;

create view step3 as
select * from step1
union
select * from step2
order by pass_id;

create view step4 as -- final answer
select pass_id,
    name,
    sum(airlines) as airlines
from step3
group by pass_id,
    name
order by pass_id;

-- Your query that answers the question goes below the "insert into" line:

INSERT INTO q1
select * from step4;

-- auto-run views on execution of script (\i) NOTE: comment out when handing in

\! echo '\n step1'
select * from step1;
\! echo '\n step2'
select * from step2;
\! echo '\n step3'
select * from step3;
\! echo '\n step4'
select * from step4;

\! echo '\n q1'
select * from q1;
