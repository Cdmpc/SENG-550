-- Creates customers table in DB
CREATE TABLE IF NOT EXISTS customers
(
	customer_id SERIAL PRIMARY KEY,
	NAME TEXT,
	email TEXT,
	phone TEXT,
	address TEXT
);

-- Creates orders table in DB
CREATE TABLE IF NOT EXISTS orders 
(
	order_id SERIAL PRIMARY KEY,
	customer_id INT REFERENCES customers(customer_id),
	order_date DATE,
	total_amount NUMERIC,
	product_id INT, 
	product_category TEXT,
	product_name TEXT
);

-- Creates delivieries table in DB
CREATE TABLE IF NOT EXISTS deliveries
(
	delivery_id SERIAL PRIMARY KEY,
	order_id INT REFERENCES orders(order_id),
	delivery_date DATE,
	status TEXT
);
