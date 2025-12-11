CREATE TABLE weatherData AS
SELECT
    location_id,
    to_date(from_unixtime(unix_timestamp(date, 'M/d/yyyy'))) AS processed_date,
    `weather_code (wmo code)` AS weather_code,
    `temperature_2m_max (°C)` AS temperature_2m_max,
    `temperature_2m_min (°C)` AS temperature_2m_min,
    `temperature_2m_mean (°C)` AS temperature_2m_mean,
    `apparent_temperature_max (°C)` AS apparent_temperature_max,
    `apparent_temperature_min (°C)` AS apparent_temperature_min,
    `apparent_temperature_mean (°C)` AS apparent_temperature_mean,
    `daylight_duration (s)` AS daylight_duration,
    `sunshine_duration (s)` AS sunshine_duration,
    `precipitation_sum (mm)` AS precipitation_sum,
    `rain_sum (mm)` AS rain_sum,
    `precipitation_hours (h)` AS precipitation_hours,
    `wind_speed_10m_max (km/h)` AS wind_speed_10m_max,
    `wind_gusts_10m_max (km/h)` AS wind_gusts_10m_max,
    `wind_direction_10m_dominant (°)` AS wind_direction_10m_dominant,
    `shortwave_radiation_sum (MJ/m²)` AS shortwave_radiation_sum,
    `et0_fao_evapotranspiration (mm)` AS et0_fao_evapotranspiration,
    sunrise,
    sunset
FROM weatherData_tmp_view;
