-- Individual Task 1: Anabel SID:520360707
-- Main code (small dataset)

SELECT 
    l.id AS listing_id,
    l.listing_name,
    COUNT(*) AS num_reviews,
    MAX(r.review_date) AS last_review
FROM airbnb.listings_small AS l
JOIN airbnb.reviews_small AS r 
    ON l.id = r.listing_id
JOIN airbnb.cities AS c
    ON l.city_id = c.id
WHERE c.city_name = 'Melbourne'
  AND r.review_date >= CURRENT_DATE - INTERVAL '365 days'
GROUP BY l.id, l.listing_name
ORDER BY num_reviews DESC,
         l.listing_name ASC
LIMIT 5;

-- Main code (medium dataset)
SELECT 
    l.id AS listing_id,
    l.listing_name,
    COUNT(*) AS num_reviews,
    MAX(r.review_date) AS last_review
FROM airbnb.listings_medium AS l
JOIN airbnb.reviews_medium AS r 
    ON l.id = r.listing_id
JOIN airbnb.cities AS c
    ON l.city_id = c.id
WHERE c.city_name = 'Melbourne'
  AND r.review_date >= CURRENT_DATE - INTERVAL '365 days'
GROUP BY l.id, l.listing_name
ORDER BY num_reviews DESC,
         l.listing_name ASC
LIMIT 5;

-- Index Tuning
CREATE INDEX IF NOT EXISTS idx_reviews_medium_listingid_reviewdate
    ON airbnb.reviews_medium (listing_id, review_date);



--Individual Task 2: Hieu SID:520133684
-- Small Dataset:

SELECT qualified.id,  qualified.host_name, count(DISTINCT qualified.listing_id) as number_of_listings
FROM (
    SELECT a.id, a.host_name, b.id as  listing_id
     FROM  AirBnb.Hosts a
    JOIN AirBnb.Listings_small b on a.id = b.host_id
     WHERE a.acceptance_rate like '100%' and b.room_type like 'Private room'
     and EXISTS (select 1 from AirBnb.Reviews_small c where c.listing_id = b.id)
    ) as qualified
GROUP by qualified.id, qualified.host_name
Order by count(distinct qualified.listing_id) desc, qualified.host_name asc
LIMIT 10;

-- Medium Dataset:
SELECT qualified.id,  qualified.host_name, count(DISTINCT qualified.listing_id) as number_of_listings
FROM (
    SELECT a.id, a.host_name, b.id as  listing_id
     FROM  AirBnb.Hosts a
    JOIN AirBnb.Listings_small b on a.id = b.host_id
     WHERE a.acceptance_rate like '100%' and b.room_type like 'Private room'
     and EXISTS (select 1 from AirBnb.Reviews_small c where c.listing_id = b.id)
    ) as qualified
GROUP by qualified.id, qualified.host_name
Order by count(distinct qualified.listing_id) desc, qualified.host_name asc
LIMIT 10;

-- Index Tuning
Create index reviews_list_index on AirBnb.Reviews_medium(listings_id);


--Individual task 3: Hamza SID:520636761
-- Medium Dataset
SELECT
r.reviewer_id,
r. reviewer_name,
COUNT (*) As num_reviews,
MAX (r.review_date) AS last_review
FROM AirBnB.Reviews_Medium r
JOIN AirBnB.Listings_Medium l ON r.listing_id = l.id
JOIN AirBnB.Cities c ON l.city_id = c.id
WHERE c.city_name = 'Berlin'
GROUP BY r.reviewer_id, r.reviewer_name
ORDER BY num_reviews DESC
LIMIT 3;

--Small Dataset
SELECT
r.reviewer_id,
r. reviewer_name,
COUNT (*) As num_reviews,
MAX (r.review_date) AS last_review
FROM AirBnB.Reviews_small r
JOIN AirBnB.Listings_small l ON r.listing_id = l.id
JOIN AirBnB.Cities c ON l.city_id = c.id
WHERE c.city_name = 'Berlin'
GROUP BY r.reviewer_id, r.reviewer_name
ORDER BY num_reviews DESC
LIMIT 3;
-- Index Tuning:
CREATE INDEX idx_reviews_listing_id ON AirBnB.Reviews_Medium(listing_id);




-- Team Task 1: Anabel SID:520360707
-- Main code 
WITH complaint_listings AS (
    SELECT
        r.listing_id,
        COUNT(*) AS complaint_count
    FROM airbnb.reviews_medium AS r
    WHERE EXTRACT(YEAR FROM r.review_date) IN (2024, 2025)
      AND r.comments ILIKE '%complaint%'
    GROUP BY r.listing_id
)
SELECT
    c.city_name AS city,
    n.nhood_name,
    COUNT(DISTINCT l.id) AS num_complaint_listings,
    ROUND(AVG(complaint_listings.complaint_count)::numeric, 2) AS avg_complaints_per_listing
FROM complaint_listings
JOIN airbnb.listings_medium l ON complaint_listings.listing_id = l.id
JOIN airbnb.hosts h          ON l.host_id = h.id
JOIN airbnb.neighbourhoods n ON l.neighbourhood = n.id
JOIN airbnb.cities c         ON n.city_id = c.id
WHERE c.city_name = 'Sydney'
  AND h.is_superhost = 't'
  AND l.price > 100
GROUP BY c.city_name, n.nhood_name
ORDER BY num_complaint_listings DESC
LIMIT 10;



-- Index Tuning (Small)
-- Index for filtering on review_date
CREATE INDEX idx_reviews_small_review_date
    ON airbnb.reviews_small (review_date);

-- Enable the pg_trgm extension 
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Index for improving ILIKE search on comments
CREATE INDEX idx_reviews_small_comments_gin
    ON airbnb.reviews_small USING GIN (comments gin_trgm_ops);

-- Index for filtering on price in listings_small
CREATE INDEX idx_listings_small_price
    ON airbnb.listings_small (price);

-- Index for optimizing join on neighbourhood in listings_small
CREATE INDEX idx_listings_small_neighbourhood
    ON airbnb.listings_small (neighbourhood);

-- Index Tuning (Medium)
-- CREATE INDEX idx_reviews_medium_review_date
    ON airbnb.reviews_medium (review_date);

-- CREATE INDEX idx_reviews_medium_comments_gin
    ON airbnb.reviews_medium USING GIN (comments gin_trgm_ops);

-- CREATE INDEX idx_listings_medium_price
    ON airbnb.listings_medium (price);

-- CREATE INDEX idx_listings_medium_neighbourhood
    ON airbnb.listings_medium (neighbourhood);

-- Index Tuning (Large)
-- CREATE INDEX idx_reviews_large_review_date
    ON airbnb.reviews_large (review_date);

-- CREATE INDEX idx_reviews_large_comments_gin
    ON airbnb.reviews_large USING GIN (comments gin_trgm_ops);

-- CREATE INDEX idx_listings_large_price
    ON airbnb.listings_large (price);

-- CREATE INDEX idx_listings_large_neighbourhood
    ON airbnb.listings_large (neighbourhood);

-- CREATE INDEX IF NOT EXISTS idx_hosts_is_superhost
    ON airbnb.hosts (is_superhost);


-- Code for Before and After Tuning (Small)
EXPLAIN ANALYZE
WITH complaint_listings AS (
    SELECT
        r.listing_id,
        COUNT(*) AS complaint_count
    FROM airbnb.reviews_small AS r
    WHERE EXTRACT(YEAR FROM r.review_date) IN (2024, 2025)
      AND r.comments ILIKE '%complaint%'
    GROUP BY r.listing_id
)
SELECT
    c.city_name AS city,
    n.nhood_name,
    COUNT(DISTINCT l.id) AS num_complaint_listings,
    ROUND(AVG(cl.complaint_count)::numeric, 2) AS avg_complaints_per_listing
FROM complaint_listings cl
JOIN airbnb.listings_small l ON cl.listing_id = l.id
JOIN airbnb.hosts h          ON l.host_id = h.id
JOIN airbnb.neighbourhoods n ON l.neighbourhood = n.id
JOIN airbnb.cities c         ON n.city_id = c.id
WHERE c.city_name = 'Sydney'
  AND h.is_superhost = 't'
  AND l.price > 100
GROUP BY c.city_name, n.nhood_name
ORDER BY num_complaint_listings DESC
LIMIT 10;


-- Code for Before and After Tuning (Medium)
EXPLAIN ANALYZE
WITH complaint_listings AS (
    SELECT
        r.listing_id,
        COUNT(*) AS complaint_count
    FROM airbnb.reviews_medium AS r
    WHERE EXTRACT(YEAR FROM r.review_date) IN (2024, 2025)
      AND r.comments ILIKE '%complaint%'
    GROUP BY r.listing_id
)
SELECT
    c.city_name AS city,
    n.nhood_name,
    COUNT(DISTINCT l.id) AS num_complaint_listings,
    ROUND(AVG(cl.complaint_count)::numeric, 2) AS avg_complaints_per_listing
FROM complaint_listings cl
JOIN airbnb.listings_medium l ON cl.listing_id = l.id
JOIN airbnb.hosts h          ON l.host_id = h.id
JOIN airbnb.neighbourhoods n ON l.neighbourhood = n.id
JOIN airbnb.cities c         ON n.city_id = c.id
WHERE c.city_name = 'Sydney'
  AND h.is_superhost = 't'
  AND l.price > 100
GROUP BY c.city_name, n.nhood_name
ORDER BY num_complaint_listings DESC
LIMIT 10;

-- Code for Before and After Tuning (Large)
EXPLAIN ANALYZE
WITH complaint_listings AS (
    SELECT
        r.listing_id,
        COUNT(*) AS complaint_count
    FROM airbnb.reviews_large AS r
    WHERE EXTRACT(YEAR FROM r.review_date) IN (2024, 2025)
      AND r.comments ILIKE '%complaint%'
    GROUP BY r.listing_id
)
SELECT
    c.city_name AS city,
    n.nhood_name,
    COUNT(DISTINCT l.id) AS num_complaint_listings,
    ROUND(AVG(cl.complaint_count)::numeric, 2) AS avg_complaints_per_listing
FROM complaint_listings cl
JOIN airbnb.listings_large l ON cl.listing_id = l.id
JOIN airbnb.hosts h          ON l.host_id = h.id
JOIN airbnb.neighbourhoods n ON l.neighbourhood = n.id
JOIN airbnb.cities c         ON n.city_id = c.id
WHERE c.city_name = 'Sydney'
  AND h.is_superhost = 't'
  AND l.price > 100
GROUP BY c.city_name, n.nhood_name
ORDER BY num_complaint_listings DESC
LIMIT 10;





--Team Task 2: Hamza SID:520636761, Hieu SID:520133684
--Large Dataset
WITH review_counts AS (
	SELECT
    	l.id AS listing_id,
    	c.city_name,
    	l.amenities,
    	COUNT(r.id) AS num_reviews
	FROM AirBnB.Reviews_large r
	JOIN AirBnB.Listings_large l ON r.listing_id = l.id
	JOIN AirBnB.Cities c ON l.city_id = c.id
	GROUP BY l.id, c.city_name, l.amenities
),
ranked_listings AS (
	SELECT *,
       	PERCENT_RANK() OVER (PARTITION BY city_name ORDER BY num_reviews DESC) AS rank
	FROM review_counts
),
popular_oceanview_listings AS (
	SELECT *
	FROM ranked_listings
	WHERE rank <= 0.2 AND amenities @> ARRAY['Ocean view']
)
SELECT
	city_name,
	COUNT(*) AS num_popular_oceanview_listings
FROM popular_oceanview_listings
GROUP BY city_name
ORDER BY city_name;

--Medium dataset
WITH review_counts AS (
	SELECT
    	l.id AS listing_id,
    	c.city_name,
    	l.amenities,
    	COUNT(r.id) AS num_reviews
	FROM AirBnB.Reviews_medium r
	JOIN AirBnB.Listings_medium l ON r.listing_id = l.id
	JOIN AirBnB.Cities c ON l.city_id = c.id
	GROUP BY l.id, c.city_name, l.amenities
),
ranked_listings AS (
	SELECT *,
       	PERCENT_RANK() OVER (PARTITION BY city_name ORDER BY num_reviews DESC) AS rank
	FROM review_counts
),
popular_oceanview_listings AS (
	SELECT *
	FROM ranked_listings
	WHERE rank <= 0.2 AND amenities @> ARRAY['Ocean view']
)
SELECT
	city_name,
	COUNT(*) AS num_popular_oceanview_listings
FROM popular_oceanview_listings
GROUP BY city_name
ORDER BY city_name;

--Small Dataset
WITH review_counts AS (
	SELECT
    	l.id AS listing_id,
    	c.city_name,
    	l.amenities,
    	COUNT(r.id) AS num_reviews
	FROM AirBnB.Reviews_small r
	JOIN AirBnB.Listings_small l ON r.listing_id = l.id
	JOIN AirBnB.Cities c ON l.city_id = c.id
	GROUP BY l.id, c.city_name, l.amenities
),
ranked_listings AS (
	SELECT *,
       	PERCENT_RANK() OVER (PARTITION BY city_name ORDER BY num_reviews DESC) AS rank
	FROM review_counts
),
popular_oceanview_listings AS (
	SELECT *
	FROM ranked_listings
	WHERE rank <= 0.2 AND amenities @> ARRAY['Ocean view']
)
SELECT
	city_name,
	COUNT(*) AS num_popular_oceanview_listings
FROM popular_oceanview_listings
GROUP BY city_name
ORDER BY city_name;


--Index Tuning
--Index 1, on listing_id
CREATE INDEX idx_reviews_large_listingid_id ON airbnb.reviews_large(listing_id, id);

--Gin Index
CREATE INDEX idx__amenities ON airbnb.listings_large USING GIN(amenities);
 
--Index on city_id
CREATE INDEX idx_cityid ON airbnb.listings_large(city_id);


-- Team Task 3: FOR TEAM Q1 (Everyone)

-- SQL Code (Large Dataset)
WITH complaint_listings AS (
   SELECT
       r.listing_id,
       COUNT(*) AS complaint_count
   FROM reviews_large AS r
   WHERE EXTRACT(YEAR FROM r.review_date) IN (2024, 2025)
     AND r.comments ILIKE '%complaint%'
   GROUP BY r.listing_id
)
SELECT
   c.city_name AS city,
   n.nhood_name,
   COUNT(DISTINCT l.id) AS num_complaint_listings,
   ROUND(AVG(cl.complaint_count)::numeric, 2) AS avg_complaints_per_listing
FROM complaint_listings cl
JOIN listings_large l ON cl.listing_id = l.id
JOIN hosts h          ON l.host_id = h.id
JOIN neighbourhoods n ON l.neighbourhood = n.id
JOIN cities c         ON n.city_id = c.id
WHERE c.city_name = 'Sydney'
 AND h.is_superhost = 't'
 AND l.price > 100
GROUP BY c.city_name, n.nhood_name
ORDER BY num_complaint_listings DESC
LIMIT 10;


-- Databricks Code (Large Dataset)
'''
%%python
df_listings_large = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_listings_large.csv")
df_listings_large.createOrReplaceTempView("listings_large")

df_reviews_large = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_reviews_large.csv")
df_reviews_large.createOrReplaceTempView("reviews_large")

df_hosts = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_hosts.csv")
df_hosts.createOrReplaceTempView("hosts")

df_neighbourhoods = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_neighbourhoods.csv")
df_neighbourhoods.createOrReplaceTempView("neighbourhoods")

df_cities = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_cities.csv")
df_cities.createOrReplaceTempView("cities")
'''

WITH complaint_listings AS (
   SELECT
       r.listing_id,
       COUNT(*) AS complaint_count
   FROM reviews_large AS r
   WHERE EXTRACT(YEAR FROM r.review_date) IN (2024, 2025)
     AND r.comments ILIKE '%complaint%'
   GROUP BY r.listing_id
)
SELECT
   c.city_name AS city,
   n.nhood_name,
   COUNT(DISTINCT l.id) AS num_complaint_listings,
   ROUND(AVG(cl.complaint_count)::numeric, 2) AS avg_complaints_per_listing
FROM complaint_listings cl
JOIN listings_large l ON cl.listing_id = l.id
JOIN hosts h          ON l.host_id = h.id
JOIN neighbourhoods n ON l.neighbourhood = n.id
JOIN cities c         ON n.city_id = c.id
WHERE c.city_name = 'Sydney'
 AND h.is_superhost = 't'
 AND l.price > 100
GROUP BY c.city_name, n.nhood_name
ORDER BY num_complaint_listings DESC
LIMIT 10;

'''
-- Databricks Code (Run time Optimization)
%%python
df_listings_large = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_listings_large.csv")
df_listings_large.write.format("delta").mode("overwrite").saveAsTable("listings_large_delta")

df_reviews_large = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_reviews_large.csv")
df_reviews_large.write.format("delta").mode("overwrite").saveAsTable("reviews_large_delta")

df_hosts = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_hosts.csv")
df_hosts.write.format("delta").mode("overwrite").saveAsTable("hosts_delta")

df_neighbourhoods = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_neighbourhoods.csv")
df_neighbourhoods.write.format("delta").mode("overwrite").saveAsTable("neighbourhoods_delta")

df_cities = spark.read.option("header", "true").csv("/FileStore/tables/airbnb_cities.csv")
df_cities.write.format("delta").mode("overwrite").saveAsTable("cities_delta")
'''

OPTIMIZE listings_large_delta
ZORDER BY (neighbourhood, price);

WITH complaint_listings AS (
   SELECT
       listing_id,
       COUNT(*) AS complaint_count
   FROM reviews_large_delta
   WHERE year(review_date) IN (2024, 2025)
     AND lower(comments) LIKE '%complaint%'
   GROUP BY listing_id
)
SELECT
   c.city_name AS city,
   n.nhood_name,
   COUNT(DISTINCT l.id) AS num_complaint_listings,
   ROUND(AVG(cl.complaint_count), 2) AS avg_complaints_per_listing
FROM complaint_listings cl
JOIN listings_large_delta l ON cl.listing_id = l.id
JOIN hosts_delta h ON l.host_id = h.id
JOIN neighbourhoods_delta n ON l.neighbourhood = n.id
JOIN cities_delta c ON n.city_id = c.id
WHERE c.city_name = 'Sydney'
 AND h.is_superhost = true
 AND cast(l.price as int) > 100
GROUP BY c.city_name, n.nhood_name
ORDER BY num_complaint_listings DESC
LIMIT 10;



