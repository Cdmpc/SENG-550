-- 1. Count the number of different cities each customer has lived in.
SELECT dc.name, dc.city, COUNT(dc.city) OVER(PARTITION BY dc.name)
FROM dim_customers AS dc
GROUP BY dc.name, dc.city;

-- 2. Total Sold Amount per City, based on the city they lived in at the time they placed the order.
-- COALESCE(column, value_if_column_is_null), basically replaces a null value with the value in the
-- second argument.
SELECT dc.customer_id, dc.name, dc.city, SUM(fo.amount) AS TOTAL_AMOUNT
FROM fact_orders AS fo
INNER JOIN dim_customers AS dc
	ON dc.customer_id = fo.customer_id
	AND fo.order_date BETWEEN dc.valid_start_date AND
	-- This COALESCE basically tells you replace end_date, with the current time as of now.
	-- This makes it so that cases where end_date = NULL, has a "value" to be included.
	-- in this query.
		COALESCE(dc.valid_end_date, CURRENT_TIMESTAMP)
GROUP BY dc.customer_id, dc.name, dc.city;

-- 3. Sum of (price-amount), at the time of the order, binned by group_id.
SELECT dp.product_id, dp.name, SUM((dp.price - fo.amount)) AS sum_of_discounts
FROM fact_orders as fo
INNER JOIN dim_products as dp
	ON fo.product_id = dp.product_id
	AND fo.order_date BETWEEN dp.valid_start_date 
	AND COALESCE(dp.valid_end_date, CURRENT_TIMESTAMP)
GROUP BY dp.product_id, dp.name;

/* TO SHOW ALL THE RELEVENT AMOUNT AND PRICES, TO SEE WHAT DISCOUNT APPLIES. */
-- SELECT dp.product_id, dp.name, dp.price, fo.amount, SUM((dp.price - fo.amount))
-- OVER(PARTITION BY dp.product_id)
-- FROM fact_orders as fo
-- INNER JOIN dim_products as dp
-- 	ON fo.product_id = dp.product_id
-- 	AND fo.order_date 
-- 	-- Satisfies "at the time of the order" constraint.
-- 	BETWEEN dp.valid_start_date AND COALESCE(dp.valid_end_date, CURRENT_TIMESTAMP)
-- GROUP BY dp.product_id, dp.name, dp.price, fo.amount;


-- 4. Return all orders for a specific customer, including order_id,
-- order_date, customer_city (at the time of order), product_name, product_price, amount
-- Join and return the relationship attributes requested from customers and orders
DROP TABLE IF EXISTS customer_orders CASCADE;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_orders AS
SELECT dc.customer_id, dc.name, dc.city, fo.order_id, fo.order_date, fo.amount
FROM dim_customers AS dc
INNER JOIN fact_orders AS fo
	ON fo.customer_id = dc.customer_id
	AND fo.order_date BETWEEN dc.valid_start_date
	AND COALESCE(dc.valid_end_date, CURRENT_TIMESTAMP);
-- Rename the column, because otherwise it will get deleted in the natural join
-- and it helps to differentiate it from "name" in dim_products.
ALTER TABLE customer_orders
RENAME COLUMN name TO customer_name;
SELECT *
FROM customer_orders;

-- Join and return the relationship attributes requested from products and orders
DROP TABLE IF EXISTS product_orders CASCADE;
CREATE TEMPORARY TABLE IF NOT EXISTS product_orders AS
SELECT dp.product_id, dp.name, dp.price, fo.order_id, fo.order_date, fo.amount
FROM dim_products AS dp
INNER JOIN fact_orders AS fo
	ON dp.product_id = fo.product_id
	AND fo.order_date BETWEEN dp.valid_start_date
	AND COALESCE(dp.valid_end_date, CURRENT_TIMESTAMP);

ALTER TABLE product_orders
RENAME COLUMN name TO product_name;
SELECT *
FROM product_orders;

-- Join customer_orders and product_orders ON the order columns, since they are the same
-- in both temp tables.
SELECT order_id, order_date, customer_id, customer_name, city AS customer_city, 
product_id, product_name, price AS product_price, amount
FROM customer_orders AS co
NATURAL JOIN product_orders
WHERE customer_id = 1;
	