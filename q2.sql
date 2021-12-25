-- Q2. Refunds!

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q2 CASCADE;

CREATE TABLE q2 (
    airline CHAR(2),
    name VARCHAR(50),
    year CHAR(4),
    seat_class seat_class,
    refund REAL
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.

DROP VIEW IF EXISTS step1 CASCADE;
drop view if exists step2 cascade;
drop view if exists step3 cascade;

-- Define views for your intermediate steps here:

create view step1 as
select Airline.code as airline,
    Airline.name as name,
    extract(year from Departure.datetime) as year,
    Booking.seat_class as seat_class,
    sum(case
        when floor((extract(epoch from Arrival.datetime) - extract(epoch from Flight.s_arv))/3600) <=
        floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600)/2
            then 0
        when floor((extract(epoch from Arrival.datetime) - extract(epoch from Flight.s_arv))/3600) >
        floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600)/2
        and floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600) >= 10
            then Booking.price * 0.5
        when floor((extract(epoch from Arrival.datetime) - extract(epoch from Flight.s_arv))/3600) >
        floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600)/2
        and floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600) >= 4
            then Booking.price * 0.35
        else 0
        end) as refund
from Flight
    join Departure on Flight.id = Departure.flight_id
    join Arrival on Flight.id = Arrival.flight_id
    join Booking on Departure.flight_id = Booking.flight_id
    join Airline on Flight.airline = Airline.code
    join Airport DepartureAirport on Flight.outbound = DepartureAirport.code
    join Airport ArrivalAirport on Flight.inbound = ArrivalAirport.code
where DepartureAirport.country = ArrivalAirport.country
group by Airline.code,
    Departure.datetime,
    Booking.seat_class;

create view step2 as
select Airline.code as airline,
    Airline.name as name,
    extract(year from Departure.datetime) as year,
    Booking.seat_class as seat_class,
    sum(case
        when floor((extract(epoch from Arrival.datetime) - extract(epoch from Flight.s_arv))/3600) <=
        floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600)/2
            then 0
        when floor((extract(epoch from Arrival.datetime) - extract(epoch from Flight.s_arv))/3600) >
        floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600)/2
        and floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600) >= 12
            then Booking.price * 0.5
        when floor((extract(epoch from Arrival.datetime) - extract(epoch from Flight.s_arv))/3600) >
        floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600)/2
        and floor((extract(epoch from Departure.datetime) - extract(epoch from Flight.s_dep))/3600) >= 7
            then Booking.price * 0.35
        else 0
        end) as refund
from Flight
    join Departure on Flight.id = Departure.flight_id
    join Arrival on Flight.id = Arrival.flight_id
    join Booking on Departure.flight_id = Booking.flight_id
    join Airline on Flight.airline = Airline.code
    join Airport DepartureAirport on Flight.outbound = DepartureAirport.code
    join Airport ArrivalAirport on Flight.inbound = ArrivalAirport.code
where DepartureAirport.country != ArrivalAirport.country
group by Airline.code,
    Departure.datetime,
    Booking.seat_class;

create view step3 as -- final answer
select airline,
    name,
    year,
    seat_class,
    sum(refund) as refund
from (
    select * from step1
    union all
    select * from step2 ) Combined
group by airline,
    name,
    year,
    seat_class
having sum(refund) > 0
order by sum(refund);

-- Your query that answers the question goes below the "insert into" line:

INSERT INTO q2
select * from step3;

-- auto-run views on execution of script (\i) NOTE: comment out when handing in

\! echo '\n step1'
select * from step1;
\! echo '\n step2'
select * from step2;
\! echo '\n step3'
select * from step3;

\! echo '\n q2'
select * from q2;
