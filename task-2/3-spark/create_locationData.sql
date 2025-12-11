CREATE TABLE locationData
USING csv
OPTIONS (
  path "/data/raw/locationData.csv",
  header "true",
  inferSchema "true"
);
