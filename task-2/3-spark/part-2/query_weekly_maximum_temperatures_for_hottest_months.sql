WITH monthly_avg AS (
    SELECT
        year(processed_date) AS year,
        month(processed_date) AS month,
        AVG(temperature_2m_max) AS temperature_2m_max_avg
    FROM weatherData
    GROUP BY year(processed_date), month(processed_date)
),

hottest_months AS (
    SELECT year, month
    FROM (
        SELECT *,
               MAX(temperature_2m_max_avg) OVER () AS max_temp
        FROM monthly_avg
    ) AS hottest_months_with_max_temp
    WHERE temperature_2m_max_avg = max_temp
)

SELECT
    h.year,
    h.month,
    weekofyear(w.processed_date) AS week,
    MAX(w.temperature_2m_max) AS temperature_2m_max_weekly_max
FROM weatherData w
JOIN hottest_months h
ON year(w.processed_date) = h.year
AND month(w.processed_date) = h.month
GROUP BY
    h.year,
    h.month,
    week
ORDER BY
    h.year,
    h.month,
    week;
