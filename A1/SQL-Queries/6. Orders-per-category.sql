-- 6. Find the number of orders per product category.
SELECT o.product_category, COUNT(o.order_id) as num_orders
FROM orders AS o
GROUP BY o.product_category;