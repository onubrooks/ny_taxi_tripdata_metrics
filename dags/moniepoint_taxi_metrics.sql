WITH 4 AS num_weeks
SELECT
    concat(
        YEAR(pickup_date), 
        '-',
        LPAD(CAST(MONTH(pickup_date) as CHAR), 2, '0')
    ) AS Month,
    -- Use CASE WHEN to conditionally aggregate based on day_of_week
    SUM(CASE WHEN DAYOFWEEK(pickup_date) = 6 THEN 1 ELSE 0 END) / num_weeks AS sat_mean_trip_count,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 6 THEN fare_amount ELSE NULL END) AS sat_mean_fare_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 6 THEN TIMEDIFF(pickup_datetime,dropoff_datetime) ELSE NULL END) AS sat_mean_duration_per_trip,
    SUM(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN 1 ELSE 0 END) / num_weeks AS sun_mean_trip_count,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN fare_amount ELSE NULL END) AS sun_mean_fare_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN TIMEDIFF(pickup_datetime,dropoff_datetime) ELSE NULL END) AS sun_mean_duration_per_trip
FROM tripdata
WHERE pickup_date BETWEEN '2014-01-01' AND '2016-12-31'
GROUP BY YEAR(pickup_date), MONTH(pickup_date)
ORDER BY YEAR(pickup_date), MONTH(pickup_date);

-- window function
SELECT AVG(total_count) AS avg_count
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY grouping_column ORDER BY id) AS row_num,
         COUNT(*) OVER (PARTITION BY grouping_column) AS total_count
  FROM your_table
) AS counts;
