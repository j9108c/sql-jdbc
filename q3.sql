-- Q3. North and South Connections

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q3 CASCADE;

CREATE TABLE q3 (
    outbound VARCHAR(30),
    inbound VARCHAR(30),
    direct INT,
    one_con INT,
    two_con INT,
    earliest timestamp
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.

DROP VIEW IF EXISTS step1 CASCADE;
drop view if exists step2 cascade;
drop view if exists step3 cascade;
drop view if exists step4 cascade;
drop view if exists step5 cascade;
drop view if exists step6 cascade;
drop view if exists step7 cascade;
drop view if exists step8 cascade;
drop view if exists step9 cascade;
drop view if exists step10 cascade;
drop view if exists step11 cascade;
drop view if exists step12 cascade;
drop view if exists step13 cascade;
drop view if exists step14 cascade;
drop view if exists step15 cascade;
drop view if exists step16 cascade;
drop view if exists step17 cascade;
drop view if exists step18 cascade;
drop view if exists step19 cascade;
drop view if exists step20 cascade;
drop view if exists step21 cascade;
drop view if exists step22 cascade;
drop view if exists step23 cascade;

-- Define views for your intermediate steps here:

create view step1 as -- all flights in the world on 2020-04-30
select A1.city as from_city,
    A2.city as to_city,
    A1.country as from_country,
    A2.country as to_country,
    Flight.s_dep as departure_time,
    Flight.s_arv as arrival_time
from Flight
    join Airport A1 on Flight.outbound = A1.code
    join Airport A2 on Flight.inbound = A2.code
where to_char(Flight.s_dep, 'YYYY-MM-DD') = '2020-04-30'
    and to_char(Flight.s_arv, 'YYYY-MM-DD') = '2020-04-30';

-- canada to usa

create view step2 as -- direct flights from canada cities to usa cities
select from_city,
    to_city,
    departure_time,
    arrival_time
from step1
where from_country = 'Canada'
    and to_country = 'USA';

create view step3 as -- answer for direct
select from_city,
    to_city,
    count(distinct (from_city, to_city)) as num_direct,
    0 as num_1_conn,
    0 as num_2_conn,
    min(arrival_time) as earliest_arrival_time
from step2
group by from_city,
    to_city;

create view step4 as -- flights from canada cities to any cities in the world
select from_city,
    to_city,
    departure_time,
    arrival_time
from step1
where from_country = 'Canada';

create view step5 as -- flights from any cities in the world to usa cities
select from_city,
    to_city,
    departure_time,
    arrival_time
from step1
where to_country = 'USA';

create view step6 as -- 1-connection flights from canada cities to usa cities
select step4.from_city as from_city,
    step4.to_city as conn_city,
    step5.to_city as to_city,
    step4.departure_time as departure_time,
    step5.arrival_time as arrival_time
from step4
    join step5 on step4.to_city = step5.from_city
where (step4.arrival_time + interval '30 min') <= (step5.departure_time);

create view step7 as -- answer for one_con
select from_city,
    to_city,
    0 as num_direct,
    count(distinct conn_city) as num_1_conn,
    0 as num_2_conn,
    min(arrival_time) as earliest_arrival_time
from step6
group by from_city,
    to_city;

create view step8 as -- flights from any cities in the world to any cities in the world
select from_city,
    to_city,
    departure_time,
    arrival_time
from step1;

create view step9 as -- 2-connection flights from canada cities to usa cities
select step4.from_city as from_city,
    step8.from_city as conn_city_1,
    step8.to_city as conn_city_2,
    step5.to_city as to_city,
    step4.departure_time as departure_time,
    step5.arrival_time as arrival_time
from step4
    join step8 on step4.to_city = step8.from_city
    join step5 on step8.to_city = step5.from_city
where (step4.arrival_time + interval '30 min') <= (step8.departure_time)
    and (step8.arrival_time + interval '30 min') <= (step5.departure_time);

create view step10 as -- answer for two_con
select from_city,
    to_city,
    0 as num_direct,
    0 as num_1_conn,
    count(distinct (conn_city_1, conn_city_2)) as num_2_conn,
    min(arrival_time) as earliest_arrival_time
from step9
group by from_city,
    to_city;

create view step11 as -- answer for flights canada to usa
select * from step3
union
select * from step7
union
select * from step10
order by from_city,
    to_city;

-- usa to canada

create view step12 as -- direct flights from usa cities to canada cities
select from_city,
    to_city,
    departure_time,
    arrival_time
from step1
where from_country = 'USA'
    and to_country = 'Canada';

create view step13 as -- answer for direct
select from_city,
    to_city,
    count(distinct (from_city, to_city)) as num_direct,
    0 as num_1_conn,
    0 as num_2_conn,
    min(arrival_time) as earliest_arrival_time
from step12
group by from_city,
    to_city;

create view step14 as -- flights from usa cities to any cities in the world
select from_city,
    to_city,
    departure_time,
    arrival_time
from step1
where from_country = 'USA';

create view step15 as -- flights from any cities in the world to canada cities
select from_city,
    to_city,
    departure_time,
    arrival_time
from step1
where to_country = 'Canada';

create view step16 as -- 1-connection flights from usa cities to canada cities
select step14.from_city as from_city,
    step14.to_city as conn_city,
    step15.to_city as to_city,
    step14.departure_time as departure_time,
    step15.arrival_time as arrival_time
from step14
    join step15 on step14.to_city = step15.from_city
where (step14.arrival_time + interval '30 min') <= (step15.departure_time);

create view step17 as -- answer for one_con
select from_city,
    to_city,
    0 as num_direct,
    count(distinct conn_city) as num_1_conn,
    0 as num_2_conn,
    min(arrival_time) as earliest_arrival_time
from step16
group by from_city,
    to_city;

create view step18 as -- flights from any cities in the world to any cities in the world
select from_city,
    to_city,
    departure_time,
    arrival_time
from step1;

create view step19 as -- 2-connection flights from usa cities to canada cities
select step14.from_city as from_city,
    step18.from_city as conn_city_1,
    step18.to_city as conn_city_2,
    step15.to_city as to_city,
    step14.departure_time as departure_time,
    step15.arrival_time as arrival_time
from step14
    join step18 on step14.to_city = step18.from_city
    join step15 on step18.to_city = step15.from_city
where (step14.arrival_time + interval '30 min') <= (step18.departure_time)
    and (step18.arrival_time + interval '30 min') <= (step15.departure_time);

create view step20 as -- answer for two_con
select from_city,
    to_city,
    0 as num_direct,
    0 as num_1_conn,
    count(distinct (conn_city_1, conn_city_2)) as num_2_conn,
    min(arrival_time) as earliest_arrival_time
from step19
group by from_city,
    to_city;

create view step21 as -- answer for flights usa to canada
select * from step13
union
select * from step17
union
select * from step20
order by from_city,
    to_city;

--

create view step22 as -- union the answers (canada to usa) and (usa to canada)
select * from step11
union
select * from step21;

create view step23 as -- final answer
select from_city as outbound,
    to_city as inbound,
    sum(num_direct) as direct,
    sum(num_1_conn) as one_con,
    sum(num_2_conn) as two_con,
    min(earliest_arrival_time) as earliest
from step22
group by from_city,
    to_city;

-- Your query that answers the question goes below the "insert into" line:

INSERT INTO q3
select * from step23;

-- auto-run views on execution of script (\i) NOTE: comment out when handing in

\! echo '\n step1'
select * from step1;
\! echo '\n step2'
select * from step2;
\! echo '\n step3'
select * from step3;
\! echo '\n step4'
select * from step4;
\! echo '\n step5'
select * from step5;
\! echo '\n step6'
select * from step6;
\! echo '\n step7'
select * from step7;
\! echo '\n step8'
select * from step8;
\! echo '\n step9'
select * from step9;
\! echo '\n step10'
select * from step10;
\! echo '\n step11'
select * from step11;
\! echo '\n step12'
select * from step12;
\! echo '\n step13'
select * from step13;
\! echo '\n step14'
select * from step14;
\! echo '\n step15'
select * from step15;
\! echo '\n step16'
select * from step16;
\! echo '\n step17'
select * from step17;
\! echo '\n step18'
select * from step18;
\! echo '\n step19'
select * from step19;
\! echo '\n step20'
select * from step20;
\! echo '\n step21'
select * from step21;
\! echo '\n step22'
select * from step22;
\! echo '\n step23'
select * from step23;

\! echo '\n q3'
select * from q3;
