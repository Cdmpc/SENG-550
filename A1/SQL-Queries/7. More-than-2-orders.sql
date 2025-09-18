-- 7. Find customers who placed more than 2 orders.
SELECT c.name, COUNT(o.customer_id)
FROM customers AS c
INNER JOIN orders AS o
	USING (customer_id)
GROUP BY c.name
HAVING COUNT(o.customer_id) > 2;

