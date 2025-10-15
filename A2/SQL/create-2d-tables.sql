-- CUSTOMERS SCD TYPE 2 TABLE.
CREATE TABLE IF NOT EXISTS dim_customers
(
	-- Surrogate Key and Customer Key, id = Surrogate Key is the primary key
	-- Customer_id is the business key, NOT NULL only as customers can be duplicated with new records.
	id SERIAL PRIMARY KEY,
	customer_id INT NOT NULL,
	name TEXT NOT NULL,
	email TEXT,
	city TEXT,
	-- This shows when the data was valid, if end_date is NULL, this means
	-- that the row is the MOST RECENT data for this entity. CURRENT_TIMESTAMP extracts the
	-- current date and time, which will be the defaule
	valid_start_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
	valid_end_date TIMESTAMP,
	UNIQUE(customer_id, name, email, city)
);

-- PRODUCTS SCD TYPE 2 TABLE.
CREATE TABLE IF NOT EXISTS dim_products
(
	id SERIAL PRIMARY KEY, -- product surrogate key
	product_id INT NOT NULL, -- product business key, can be duplicated.
	name TEXT NOT NULL,
	category TEXT,
	price NUMERIC,
	valid_start_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
	valid_end_date TIMESTAMP,
	UNIQUE(product_id, name, category, price)
);

-- ORDERS FACT TABLE
-- Fact tables ALWAYS LINK TO THE CURRENT most recent data they link to.
-- The dim tables have FOREIGN KEYS in the fact tables.
CREATE TABLE IF NOT EXISTS fact_orders
(
	order_id SERIAL PRIMARY KEY,
	product_id INT NOT NULL, -- Foreign Key to dim_product surrogate key
	customer_id INT NOT NULL, -- Foreign Key to dim_customer surrogate key
	order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
	-- amount is the actual price paid, due to discounts or other special offers.
	amount NUMERIC,
	UNIQUE(product_id, customer_id, amount),
	-- Create the FK references to the dim table surrogate keys.
	FOREIGN KEY (product_id) REFERENCES dim_products(id),
	FOREIGN KEY (customer_id) REFERENCES dim_customers(id)
);