-- Q5. Flight Hopping

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q5 CASCADE;

CREATE TABLE q5 (
	destination CHAR(3),
	num_flights INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.

DROP VIEW IF EXISTS day CASCADE;
DROP VIEW IF EXISTS n CASCADE;

CREATE VIEW day AS
SELECT day::date as day FROM q5_parameters;
-- can get the given date using: (SELECT day from day)

CREATE VIEW n AS
SELECT n FROM q5_parameters;
-- can get the given number of flights using: (SELECT n from n)

-- HINT: You can answer the question by writing one recursive query below, without any more views.
-- Your query that answers the question goes below the "insert into" line:

INSERT INTO q5

-- recursive CTE
with recursive hopping as (
	-- base case
	(select 'YYZ'::text as destination,
		0 as num_flights,
		day as date
	from q5_parameters)

	union all -- adds all intermediate relations upon termination

	-- recursive case
	(select Flight.inbound as destination,
		(hopping.num_flights + 1) as num_flights,
		Flight.s_arv as date
	from hopping
		join Flight on hopping.destination = Flight.outbound
	where num_flights < (select n from q5_parameters) -- terminating condition
		and (Flight.s_dep - hopping.date) < '24:00:00')
)

select destination, num_flights from hopping order by destination, num_flights;

-- auto-run views on execution of script (\i) NOTE: comment out when handing in

\! echo '\n q5'
select * from q5;
