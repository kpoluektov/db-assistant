-- Test data for the demo schema.
-- Idempotent: TRUNCATEs all tables before inserting so it can be re-run.
-- Volume: 10 airports, 12 flights, 31 days of planned flights (~372 rows),
-- ~120 passengers per non-cancelled flight (~38k boardings).

SET search_path TO demo;

TRUNCATE TABLE demo.boardings,
               demo.planned_flights,
               demo.flights,
               demo.statuses,
               demo.plane_types,
               demo.airports
    RESTART IDENTITY CASCADE;

-- airports -------------------------------------------------------------------
INSERT INTO demo.airports (id, city, country, active) VALUES
    ('SVO', 'Moscow',          'RU', true),
    ('DME', 'Moscow',          'RU', true),
    ('LED', 'Saint Petersburg','RU', true),
    ('KZN', 'Kazan',           'RU', true),
    ('AER', 'Sochi',           'RU', true),
    ('OVB', 'Novosibirsk',     'RU', true),
    ('SVX', 'Yekaterinburg',   'RU', true),
    ('KJA', 'Krasnoyarsk',     'RU', true),
    ('VVO', 'Vladivostok',     'RU', true),
    ('KGD', 'Kaliningrad',     'RU', false);

-- plane_types ----------------------------------------------------------------
INSERT INTO demo.plane_types (code, description) VALUES
    ('SU95',   'Sukhoi Superjet 100, ~100 seats'),
    ('A320',   'Airbus A320, ~150 seats'),
    ('A321',   'Airbus A321, ~200 seats'),
    ('B737',   'Boeing 737-800, ~180 seats'),
    ('B777',   'Boeing 777-300ER, ~400 seats'),
    ('IL96',   'Ilyushin Il-96, ~300 seats');

-- statuses -------------------------------------------------------------------
INSERT INTO demo.statuses (id, name) VALUES
    ('SCHEDULED', 'Scheduled'),
    ('BOARDING',  'Boarding in progress'),
    ('DEPARTED',  'Departed on time'),
    ('DELAYED',   'Delayed'),
    ('ARRIVED',   'Arrived at destination'),
    ('CANCELLED', 'Cancelled');

-- flights --------------------------------------------------------------------
INSERT INTO demo.flights (id, src, dst, takeoff_time) VALUES
    ('SU0001', 'SVO', 'LED', '07:30'),
    ('SU0002', 'LED', 'SVO', '10:15'),
    ('SU0010', 'SVO', 'AER', '06:45'),
    ('SU0011', 'AER', 'SVO', '11:20'),
    ('SU0020', 'DME', 'KZN', '08:00'),
    ('SU0021', 'KZN', 'DME', '12:30'),
    ('SU0030', 'SVO', 'OVB', '22:10'),
    ('SU0031', 'OVB', 'SVO', '04:50'),
    ('SU0040', 'SVO', 'SVX', '09:00'),
    ('SU0041', 'SVX', 'SVO', '14:35'),
    ('SU0050', 'DME', 'VVO', '20:30'),
    ('SU0051', 'VVO', 'DME', '09:40');

-- planned_flights ------------------------------------------------------------
-- 30 days starting 2026-04-01; pseudo-random plane/status assignment.
INSERT INTO demo.planned_flights (flight, flight_date, boarding_time, takeoff_time, delay_time, status, plane)
SELECT
    f.id,
    d::date AS flight_date,
    (d + f.takeoff_time - interval '30 minutes')::timestamp AS boarding_time,
    (d + f.takeoff_time)::timestamp                        AS takeoff_time,
    CASE WHEN (extract(doy FROM d)::int + length(f.id)) % 7 = 0
         THEN (d + f.takeoff_time + interval '45 minutes')::timestamp
         ELSE NULL
    END AS delay_time,
    CASE (extract(doy FROM d)::int + length(f.id)) % 7
        WHEN 0 THEN 'DELAYED'
        WHEN 1 THEN 'ARRIVED'
        WHEN 2 THEN 'ARRIVED'
        WHEN 3 THEN 'DEPARTED'
        WHEN 4 THEN 'BOARDING'
        WHEN 5 THEN 'SCHEDULED'
        ELSE        'CANCELLED'
    END AS status,
    (ARRAY['SU95','A320','A321','B737','B777','IL96'])
        [1 + (abs(hashtext(f.id || d::text)) % 6)] AS plane
FROM demo.flights f
CROSS JOIN generate_series(DATE '2026-07-01', DATE '2026-07-31', INTERVAL '1 day') AS d;

-- boardings ------------------------------------------------------------------
-- For each non-cancelled planned flight, generate ~120 passengers in seats 1A..30F.
WITH seats AS (
    SELECT row_number() OVER () AS seat_no,
           (r::text || c) AS place
    FROM generate_series(1, 30) AS r
    CROSS JOIN unnest(ARRAY['A','B','C','D','E','F']) AS c
),
candidates AS (
    SELECT pf.flight,
           pf.flight_date,
           s.seat_no,
           s.place,
           row_number() OVER (ORDER BY pf.flight_date, pf.flight, s.seat_no) AS rn
    FROM demo.planned_flights pf
    JOIN seats s ON s.seat_no <= 120
    WHERE pf.status <> 'CANCELLED'
),
first_names AS (
    SELECT * FROM (VALUES
        ('Ivan'),('Olga'),('Sergey'),('Anna'),('Dmitry'),('Maria'),
        ('Alexey'),('Elena'),('Pavel'),('Natalia'),('Mikhail'),('Tatiana'),
        ('Andrey'),('Ekaterina'),('Nikolay'),('Svetlana')
    ) AS t(name)
),
last_names AS (
    SELECT * FROM (VALUES
        ('Ivanov'),('Petrov'),('Sidorov'),('Smirnov'),('Volkov'),('Kuznetsov'),
        ('Popov'),('Vasiliev'),('Sokolov'),('Mikhailov'),('Novikov'),('Fedorov'),
        ('Morozov'),('Lebedev'),('Egorov'),('Pavlov')
    ) AS t(name)
),
fn_arr AS (SELECT array_agg(name) AS arr FROM first_names),
ln_arr AS (SELECT array_agg(name) AS arr FROM last_names)
INSERT INTO demo.boardings
    (id, flight, flight_date, first_name, last_name, id_num, reservation_number, place)
SELECT
    c.rn,
    c.flight,
    c.flight_date,
    (SELECT arr[1 + (c.rn % array_length(arr, 1))] FROM fn_arr),
    (SELECT arr[1 + ((c.rn / 3) % array_length(arr, 1))] FROM ln_arr),
    lpad(((c.rn * 7919) % 10000000000)::text, 10, '0'),
    'R' || lpad(((c.rn * 31) % 1000000)::text, 6, '0'),
    c.place
FROM candidates c;

-- Sanity output --------------------------------------------------------------
SELECT 'airports'        AS table, count(*) AS rows FROM demo.airports
UNION ALL SELECT 'plane_types',     count(*) FROM demo.plane_types
UNION ALL SELECT 'statuses',        count(*) FROM demo.statuses
UNION ALL SELECT 'flights',         count(*) FROM demo.flights
UNION ALL SELECT 'planned_flights', count(*) FROM demo.planned_flights
UNION ALL SELECT 'boardings',       count(*) FROM demo.boardings;
