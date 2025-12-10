CREATE TABLE weatherData AS
SELECT
  location_id,
  TO_DATE(STR_TO_DATE(raw_date, 'M/d/yyyy')) AS processed_date,
  weather_code,
  temperature_2m_max,
  temperature_2m_min,
  temperature_2m_mean,
  apparent_temperature_max,
  apparent_temperature_min,
  apparent_temperature_mean,
  daylight_duration,
  sunshine_duration,
  precipitation_sum,
  rain_sum,
  precipitation_hours,
  wind_speed_10m_max,
  wind_gusts_10m_max,
  wind_direction_10m_dominant,
  shortwave_radiation_sum,
  et0_fao_evapotranspiration,
  sunrise,
  sunset
FROM weatherData_raw;
