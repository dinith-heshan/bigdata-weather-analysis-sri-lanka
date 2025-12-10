SELECT
    l.city_name,
    CASE
        WHEN MONTH(w.processed_date) IN (9,10,11,12,1,2,3) THEN 'Sep-Mar'
        ELSE 'Apr-Aug'
    END AS season,
    AVG(w.et0_fao_evapotranspiration) AS et0_fao_evapotranspiration_avg
FROM weatherData w
JOIN locationData l
  ON w.location_id = l.location_id
GROUP BY
    l.city_name,
    CASE
        WHEN MONTH(w.processed_date) IN (9,10,11,12,1,2,3) THEN 'Sep-Mar'
        ELSE 'Apr-Aug'
    END;
