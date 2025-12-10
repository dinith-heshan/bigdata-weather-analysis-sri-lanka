CREATE EXTERNAL TABLE raw_weatherData (
  location_id INT,
  raw_date STRING,
  weather_code INT,
  temperature_2m_max DOUBLE,
  temperature_2m_min DOUBLE,
  temperature_2m_mean DOUBLE,
  apparent_temperature_max DOUBLE,
  apparent_temperature_min DOUBLE,
  apparent_temperature_mean DOUBLE,
  daylight_duration DOUBLE,
  sunshine_duration DOUBLE,
  precipitation_sum DOUBLE,
  rain_sum DOUBLE,
  precipitation_hours INT,
  wind_speed_10m_max DOUBLE,
  wind_gusts_10m_max DOUBLE,
  wind_direction_10m_dominant INT,
  shortwave_radiation_sum DOUBLE,
  et0_fao_evapotranspiration DOUBLE,
  sunrise STRING,
  sunset STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ","
)
STORED AS TEXTFILE
LOCATION '/data/raw/weather'
TBLPROPERTIES ("skip.header.line.count"="1");
