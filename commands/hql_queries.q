create database narinicheva;
use narinicheva;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;

CREATE EXTERNAL TABLE IF NOT EXISTS purchase_events (
product_name STRING, product_price STRING, purchase_timestamp STRING, product_category STRING, client_ip STRING
)
PARTITIONED BY (purchase_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/cloudera/events/';

ALTER TABLE purchase_events
ADD PARTITION (purchase_date='2019-03-29')
LOCATION '/user/cloudera/events/2019/03/29';

ALTER TABLE purchase_events
ADD PARTITION (purchase_date='2019-03-30')
LOCATION '/user/cloudera/events/2019/03/30';

ALTER TABLE purchase_events
ADD PARTITION (purchase_date='2019-03-31')
LOCATION '/user/cloudera/events/2019/03/31';

ALTER TABLE purchase_events
ADD PARTITION (purchase_date='2019-04-01')
LOCATION '/user/cloudera/events/2019/04/01';

ALTER TABLE purchase_events
ADD PARTITION (purchase_date='2019-04-02')
LOCATION '/user/cloudera/events/2019/04/02';

ALTER TABLE purchase_events
ADD PARTITION (purchase_date='2019-04-03')
LOCATION '/user/cloudera/events/2019/04/03';

ALTER TABLE purchase_events
ADD PARTITION (purchase_date='2019-04-04')
LOCATION '/user/cloudera/events/2019/04/04';

INSERT OVERWRITE DIRECTORY '/user/cloudera/most_frequently_purchased_categories'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT product_category, count(*) as purchase_count
FROM purchase_events
GROUP BY product_category
DISTRIBUTE BY purchase_count SORT BY purchase_count DESC, product_category ASC
LIMIT 10;

INSERT OVERWRITE DIRECTORY '/user/cloudera/most_frequently_purchased_products'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT product_category, product_name, purchase_count FROM ( SELECT * FROM
(SELECT product_category, product_name, purchase_count, 
ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY purchase_count DESC) AS row_num
FROM (SELECT product_category, product_name, count(purchase_timestamp) AS purchase_count 
FROM purchase_events GROUP BY product_category, product_name) AS T2) AS T1
WHERE row_num < 3
DISTRIBUTE BY product_category SORT BY product_category, row_num) AS T;

CREATE EXTERNAL TABLE IF NOT EXISTS geolite2_blocks
(network STRING, geoname_id INT, registered_country_geoname_id INT, represented_country_geoname_id INT, is_anonymous_proxy INT, is_satellite_provider INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/geolite2/blocks/';

ALTER TABLE geolite2_blocks SET TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS geolite2_locations
(geoname_id INT, locale_code STRING, continent_code STRING, continent_name STRING, country_iso_code STRING, country_name STRING, is_in_european_union INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/geolite2/locations/';

ALTER TABLE geolite2_locations SET TBLPROPERTIES ("skip.header.line.count"="1");

ADD JAR /media/sf_VM_Shared/sethe-final-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar;
list jars;

CREATE TEMPORARY FUNCTION convertIpToDecimal AS 'com.nataliaar.sethefinalproject.udf.IpToDecimalConverter';
CREATE TEMPORARY FUNCTION getIpRangeBoundaries AS 'com.nataliaar.sethefinalproject.udf.IpRangeBoundariesFinder';

CREATE TABLE IF NOT EXISTS geolite2_blocks_lookup_temp AS SELECT getIpRangeBoundaries(network) AS ip_range, geoname_id FROM geolite2_blocks;
CREATE TABLE IF NOT EXISTS geolite2_blocks_lookup AS 
SELECT DISTINCT ip_range[0] AS start_ip, ip_range[1] AS end_ip, country_name 
FROM geolite2_blocks_lookup_temp b INNER JOIN geolite2_locations l ON b.geoname_id = l.geoname_id
WHERE LENGTH(LTRIM(country_name)) > 0
CLUSTER BY start_ip;
DROP TABLE IF EXISTS geolite2_blocks_lookup_temp;

CREATE TABLE IF NOT EXISTS purchase_events_with_ip_decimal AS SELECT *, convertIpToDecimal(client_ip) AS client_ip_decimal FROM purchase_events;

CREATE TABLE IF NOT EXISTS purchase_events_with_country AS
SELECT /*+ MAPJOIN(geolite2_blocks_lookup) */ product_name, product_price, purchase_timestamp, product_category, client_ip, country_name
FROM geolite2_blocks_lookup JOIN purchase_events_with_ip_decimal WHERE client_ip_decimal >= start_ip AND client_ip_decimal <= end_ip;

INSERT OVERWRITE DIRECTORY '/user/cloudera/countries_with_highest_money_spending'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT /*+ STREAMTABLE(purchase_events_with_country) */ country_name, ROUND(SUM(product_price), 2) AS money_spending FROM purchase_events_with_country 
GROUP BY country_name DISTRIBUTE BY money_spending SORT BY money_spending DESC LIMIT 10;
