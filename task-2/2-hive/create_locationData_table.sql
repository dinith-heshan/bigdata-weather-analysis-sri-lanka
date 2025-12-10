CREATE EXTERNAL TABLE locationData (
  location_id INT,
  latitude DOUBLE,
  longitude DOUBLE,
  elevation INT,
  utc_offset_seconds INT,
  timezone STRING,
  timezone_abbreviation INT,
  city_name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",")
STORED AS TEXTFILE
LOCATION '/data/raw/location'
TBLPROPERTIES ("skip.header.line.count"="1");
