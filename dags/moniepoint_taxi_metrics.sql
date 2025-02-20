-- run query at https://github.demo.trial.altinity.cloud:8443/play using 'demo' as username and password.
with base_data as (
    SELECT
        pickup_datetime,
        fare_amount,
        TIMEDIFF(pickup_datetime,dropoff_datetime) AS duration,  
        COUNT(*) OVER (PARTITION BY DATE(pickup_datetime)) as trip_count -- trips per day
    FROM
        tripdata
    WHERE
        pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31'
)

SELECT
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 6 THEN trip_count ELSE NULL END) AS sat_mean_trip_count,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 6 THEN fare_amount ELSE NULL END) AS sat_mean_fare_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 6 THEN duration ELSE NULL END) AS sat_mean_duration_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 7 THEN trip_count ELSE NULL END) AS sun_mean_trip_count,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 7 THEN fare_amount ELSE NULL END) AS sun_mean_fare_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_datetime) = 7 THEN duration ELSE NULL END) AS sun_mean_duration_per_trip
FROM base_data
GROUP BY
    month
ORDER BY
    month;
