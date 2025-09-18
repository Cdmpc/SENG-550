-- 4. List all orders that have not yet been delivered (delivery status not “Delivered”)
SELECT *
FROM orders AS o
INNER JOIN deliveries AS d
	USING (order_id)
WHERE d.status != 'Delivered';
