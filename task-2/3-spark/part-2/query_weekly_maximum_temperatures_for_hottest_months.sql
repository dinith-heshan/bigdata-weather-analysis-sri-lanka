WITH monthly_avg AS (
    SELECT
        year(processed_date) AS year,
        month(processed_date) AS month,
        AVG(temperature_2m_max) AS avg_max_temp
    FROM weatherData
    GROUP BY year(processed_date), month(processed_date)
),

hottest_months AS (
    SELECT *
    FROM (
        SELECT
            year,
            month,
            avg_max_temp,
            ROW_NUMBER() OVER (PARTITION BY year ORDER BY avg_max_temp DESC) AS rn
        FROM monthly_avg
    ) t
    WHERE rn = 1   -- hottest month
)
SELECT * FROM hottest_months;





WITH monthly_avg AS (
    SELECT
        year(processed_date) AS year,
        month(processed_date) AS month,
        AVG(temperature_2m_max) AS avg_max_temp
    FROM weatherData
    GROUP BY year(processed_date), month(processed_date)
),

hottest_months AS (
    SELECT *
    FROM (
        SELECT
            year,
            month,
            avg_max_temp,
            ROW_NUMBER() OVER (PARTITION BY year ORDER BY avg_max_temp DESC) AS rn
        FROM monthly_avg
    ) t
    WHERE rn = 1
)

SELECT
    w.location_id,
    h.year,
    h.month,
    weekofyear(w.processed_date) AS week,
    MAX(w.temperature_2m_max) AS weekly_max_temperature
FROM weatherData w
JOIN hottest_months h
ON year(w.processed_date) = h.year
AND month(w.processed_date) = h.month
GROUP BY
    w.location_id,
    h.year,
    h.month,
    weekofyear(w.processed_date)
ORDER BY
    h.year,
    h.month,
    week;
