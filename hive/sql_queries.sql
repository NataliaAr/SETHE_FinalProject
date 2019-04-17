CREATE DATABASE narinicheva;

CREATE TABLE IF NOT EXISTS most_frequently_purchased_products (
	product_category VARCHAR(50) NOT NULL, 
	product_name VARCHAR(50) NOT NULL, 
	purchase_count INT,
	PRIMARY KEY (product_category, product_name)
	);
	
CREATE TABLE IF NOT EXISTS most_frequently_purchased_categories (
	product_category VARCHAR(50) NOT NULL,
	purchase_count INT,
	PRIMARY KEY (product_category)
	);
	
CREATE TABLE IF NOT EXISTS countries_with_highest_money_spending
(
	country_name VARCHAR(50) NOT NULL,
	money_spending DOUBLE(10,2),
	PRIMARY KEY (country_name)
	);