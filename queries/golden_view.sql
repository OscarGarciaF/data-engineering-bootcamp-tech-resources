CREATE OR REPLACE VIEW golden_aggs AS

WITH review_analytics AS( 
SELECT CAST(cid AS INTEGER), SUM(positive_review) AS review_score , COUNT(cid) AS review_count
FROM s3_schema.movie_reviews
WHERE cid IS NOT NULL 
GROUP BY cid),

user_analytics AS(
SELECT customer_id, CAST(SUM(quantity * unit_price) AS DECIMAL(18, 5)) AS amount_spent 
FROM s3_schema.user_purchase 
WHERE customer_id IS NOT NULL GROUP BY customer_id)

SELECT COALESCE(ua.customer_id, ra.cid) AS customer_id, COALESCE(amount_spent, 0) AS amount_spent,
COALESCE(review_score, 0) AS review_score, COALESCE(review_count, 0) AS review_count, CURRENT_DATE AS insert_date                      
FROM review_analytics ra
FULL JOIN user_analytics ua ON ra.cid = ua.customer_id
WITH NO SCHEMA BINDING;