CREATE OR REPLACE TEMPORARY VIEW locationData_tmp_view
USING csv
OPTIONS (
  path "/data/raw/locationData.csv",
  header "true",
  inferSchema "true"
);
