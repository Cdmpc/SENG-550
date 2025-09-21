-- Creating payments table, should be linked to customers and orders.
-- Customers to payments is an optional 1-1 relationship.
-- Orders to payments has a 1-M relationship, where a payment record can have multiple orders.
-- Method represents the type of method they used like Credit Card
CREATE TABLE IF NOT EXISTS Payment_Information
(
    payment_id SERIAL PRIMARY KEY,
    payment_method TEXT NOT NULL,
	card_token TEXT UNIQUE, -- Security step as databases should never store RAW card numbers nor CVVs.
    expiration_date DATE,
    customer_id INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
-- Creating membership table, to store customers who are part of
-- the online store's membership subscription service, based on it's level.
-- A customer may only have one subscription, and a subscription level
-- can be applied to many customers.
CREATE TABLE IF NOT EXISTS Membership_Subscribers
(
	member_id SERIAL PRIMARY KEY,
	customer_id INT NOT NULL,
	membership_level TEXT NOT NULL,
	FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Alter orders table to include the payment_id as a Foreign Key.
ALTER TABLE orders
ADD payment_id INT;

ALTER TABLE orders
ADD CONSTRAINT fk_payment_id
FOREIGN KEY (payment_id) REFERENCES Payment_Information(payment_id);

