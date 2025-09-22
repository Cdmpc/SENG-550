-- 3. Find all orders made by customer "Alice Johnson."
SELECT c.customer_id, c.name, o.order_id, o.order_date, o.total_amount, o.product_name
FROM customers AS c
INNER JOIN orders as o
	ON c.customer_id = o.customer_id
WHERE c.name = 'Alice Johnson';