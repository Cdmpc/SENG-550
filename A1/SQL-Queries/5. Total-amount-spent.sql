-- 5. Find the total amount spent by each customer, sorted from highest to lowest.
SELECT c.name, o.customer_id, SUM(o.total_amount) as total_spent
FROM orders AS o
INNER JOIN customers AS c
	USING (customer_id)
GROUP BY c.name, o.customer_id
ORDER BY total_spent DESC;