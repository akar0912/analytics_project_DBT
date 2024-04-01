-- create a series to generate dates from 2023-07-01 till current date
SELECT date_trunc('day', generate_series)::date AS date
FROM generate_series(
        '2023-07-01'::timestamp,
        CURRENT_DATE,
        '1 day'::interval
)
