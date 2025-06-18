-- DDL and data loading

-- Create Table: customers_raw
CREATE TABLE IF NOT EXISTS customers_raw (
customer_id INTEGER PRIMARY KEY,
name TEXT,
signup_date DATE);

-- Load Table: customers_raw from customers.csv
.mode csv
.import customers_wo_header.csv customers_raw

-- Create Table: transactions_raw
CREATE TABLE IF NOT EXISTS transactions_raw (
txn_id INTEGER PRIMARY KEY,
customer_id INTEGER,
txn_date DATE,
amount REAL
);

-- Load Table: transactions_raw from transactions.csv
.mode csv
.import transactions_wo_header.csv transactions_raw

-- Create Dimension Table: dim_customer
CREATE TABLE IF NOT EXISTS dim_customer (
customer_id INTEGER PRIMARY KEY,
name TEXT,
signup_date DATE,
signup_month INTEGER,
signup_year INTEGER
);

-- Load Dimension: dim_customer
INSERT INTO dim_customer (customer_id, name, signup_date, signup_month, signup_year)
SELECT
customer_id,
name,
signup_date,
strftime('%m', signup_date) as signup_month,
strftime('%Y', signup_date) as signup_year
from customers_raw;

-- Create Fact Table: fact_transaction
CREATE TABLE IF NOT EXISTS fact_transaction (
txn_id INTEGER PRIMARY KEY,
customer_id INTEGER REFERENCES dim_customer(customer_id),
amount REAL,
txn_date DATE,
days_since_signup INTEGER
);

-- Load Fact Table: fact_transaction
INSERT INTO fact_transaction (txn_id, customer_id, amount, txn_date, days_since_signup)
SELECT
tr.txn_id,
tr.customer_id,
tr.amount,
tr.txn_date,
CAST(julianday(tr.txn_date) - julianday(dc.signup_date) as INTEGER) as days_since_signup
FROM transactions_raw tr
LEFT JOIN dim_customer dc
ON tr.customer_id = dc.customer_id;
