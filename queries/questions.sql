--Q1
SELECT COUNT(*), location
FROM s3_schema.log_reviews
WHERE location IN ('California', 'New York', 'Texas')
GROUP BY location
ORDER BY location;

--Q2
SELECT COUNT(*), os, location
FROM s3_schema.log_reviews
WHERE location IN ('California', 'New York', 'Texas')
GROUP BY os, location
ORDER BY os, location;

--Q3
SELECT COUNT(*), device, location 
FROM s3_schema.log_reviews
WHERE device = 'Computer'
GROUP BY location, device
ORDER BY count DESC;

--Q4
WITH count_year AS (
SELECT COUNT(*), EXTRACT(YEAR from TO_DATE(logDate, 'MM-DD-YYYY')) AS year, location 
FROM s3_schema.log_reviews
WHERE year = 2021
GROUP BY location, year)

(SELECT * 
FROM count_year
ORDER BY count DESC
LIMIT 1) 
UNION
(SELECT * 
FROM count_year
ORDER BY count ASC
LIMIT 1);

--Q5
WITH log_region AS (
  SELECT COUNT(*), device, 
  CASE 
      WHEN location IN ('Alaska',
                       'Arizona',
                       'California',
                       'Colorado',
                       'Hawaii',
                       'Idaho',
                       'Montana',
                       'Nevada',
                       'New Mexico',
                       'Oregon',
                       'Utah',
                       'Washington',
                       'Wyoming')

                                  THEN 'West'
          ELSE 'East'
  END
  AS region
  FROM s3_schema.log_reviews
  GROUP BY device, region
  )
  

SELECT l.count, l.device, l.region
FROM (
  SELECT MAX(count) AS max, region
  FROM log_region
  GROUP BY region) m 
JOIN log_region l on l.count = m.max
ORDER BY count DESC;