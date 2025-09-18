-- 8. Find the product category with the highest total sales amount.
SELECT o.product_category, SUM(o.total_amount) AS total_spent
FROM orders AS o
GROUP BY o.product_category
-- Orders the categories so that the largest total amount is always on the top.
-- Then extracts ONLY that top row.
ORDER BY total_spent DESC
LIMIT 1;
