CREATE OR REPLACE TEMPORARY VIEW weather_with_month AS
SELECT
    *,
    YEAR(processed_date) AS year,
    MONTH(processed_date) AS month
FROM weatherData;

SELECT
    year,
    month,
    SUM(CASE WHEN shortwave_radiation_sum > 15 THEN shortwave_radiation_sum ELSE 0 END) * 100.0
        / SUM(shortwave_radiation_sum) AS percentage_above_15
FROM weather_with_month
GROUP BY year, month
ORDER BY year, month;

