CREATE OR REPLACE TEMPORARY VIEW weatherData_tmp_view
USING csv
OPTIONS (
  path "/data/raw/weatherData.csv",
  header "true",
  inferSchema "true"
);
