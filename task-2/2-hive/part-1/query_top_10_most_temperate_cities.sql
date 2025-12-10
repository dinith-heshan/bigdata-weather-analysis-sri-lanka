SELECT
    l.city_name,
    SUM(w.temperature_2m_max) AS temperature_2m_max_sum
FROM weatherData w
JOIN locationData l
  ON w.location_id = l.location_id
GROUP BY l.city_name
ORDER BY temperature_2m_max_sum ASC
LIMIT 10;
