# Virinderpal Singh Batth

VantageScore Data Engineer Assessment

2025-06-16

## TASK 1: ETL Pipeline

### Script Overview

This script will read customers.csv and transactions.csv files into a Pandas dataframe. It will filter out transactions older than 90 days from current time. Then it will merge with customers on the customer_id into a staging dataframe. Finally the staging dataframe is written to a sqlite3 database and table. Specific details are below.

### Assumptions

I assumed the data points needed to satisfy the basic requirements of the assignment.

### Setup & Usage

1. The csv files must be placed in the same directory as the script.
2. CSV files are read into respective dataframes.
3. Convert 'signup_date' in the customer dataframe and 'txn_date' in the transaction dataframe to datetime objects.
4. Handles conversion errors by logging and exiting the script.
5. Filter the transaction dataframe to include only transactions from the last 90 days. Firs calcualte the date 90 days ago as ```cutoff_date = current_time - 90 days```. Then ```txn_date >= cutoff_date``` is used for the filter.
6. Merge the filtered transaction dataframe (as left table) with the customer dataframe on ```customer_id```, using a left join. It is a left join because a transaction will always have a customer but a customer will not always have a transaction. This prevents potential null records in the staging dataframe.
7. Calculates the number of days since customer signup for each transaction.
```days_since_signup = txn_date - signup_date```
8. Reorders and selects relevant columns for the output.
9. Converts datetime columns to date objects to match the database schema.
10. The staging dataframe is written to a sqlite database ```VirinderpalSinghBatth_DE_Assessment.db```

### Input Dataframes

```cust_df``` DataFrame containing customer data with at least ```customer_id```, ```name``` and ```signup_date``` columns.

```txn_df``` DataFrame containing transaction data with at least ```txn_id```, ```customer_id```, ```txn_date```, and ```amount``` columns.

### Staging Dataframe

```stg_transactions_df``` Transformed and merged DataFrame with columns: ```txn_id, customer_id, txn_date, amount, signup_date, days_since_signup```

### Create Table: stg_transactions

```sql
CREATE TABLE IF NOT EXISTS stg_transactions (
txn_id INTEGER PRIMARY KEY,
customer_id INTEGER,
txn_date DATE,
amount REAL,
signup_date DATE,
days_since_signup INTEGER
)
```

### Database Info

**Database:** VirinderpalSinghBatth_DE_Assessment.db\
**Table:** stg_transactions

**Connect to Database** ```sqlite3 VirinderpalSinghBatth_DE_Assessment.db```

**Table Info** ```select name, sql from sqlite_master where name='stg_transactions';```

**Count check:**  ```select count(*) from stg_transactions;```

**View Data** ```select * from stg_transactions limit 10;```

## TASK 2: Data Modeling and Analytics

### Overview

I decided to create ```customers_raw``` and ```transactions_raw``` tables that are loaded from the csv files respectively. These serve to load the 'snapshot' data to the ```dim_customer``` and ```fact_transaction``` tables, on which the analytical queries will be ran on.

### Schema Setup

### Create Table: customers_raw

```sql
CREATE TABLE IF NOT EXISTS customers_raw (
customer_id INTEGER PRIMARY KEY,
name TEXT,
signup_date DATE
);
```

### Load Table: customers_raw from customers.csv

```sql
.mode csv
.import customers_wo_header.csv customers_raw
```

### Create Table: transactions_raw

```sql
CREATE TABLE IF NOT EXISTS transactions_raw (
txn_id INTEGER PRIMARY KEY,
customer_id INTEGER,
txn_date DATE,
amount REAL
);
```

### Load Table: transactions_raw from transactions.csv

```sql
.mode csv
.import transactions_wo_header.csv transactions_raw
```

### Create Dimension Table: dim_customer

```sql
CREATE TABLE IF NOT EXISTS dim_customer (
customer_id INTEGER PRIMARY KEY,
name TEXT,
signup_date DATE,
signup_month INTEGER,
signup_year INTEGER
);
```

### Load Dimension: dim_customer

```sql
INSERT INTO dim_customer (customer_id, name, signup_date, signup_month, signup_year)
SELECT
customer_id,
name,
signup_date,
strftime('%m', signup_date) as signup_month,
strftime('%Y', signup_date) as signup_year
from customers_raw;
```

### Create Fact Table: fact_transaction

```sql
CREATE TABLE IF NOT EXISTS fact_transaction (
txn_id INTEGER PRIMARY KEY,
customer_id INTEGER REFERENCES dim_customer(customer_id),
amount REAL,
txn_date DATE,
days_since_signup INTEGER
);
```

### Load Fact Table: fact_transaction

```sql
INSERT INTO fact_transaction (txn_id, customer_id, amount, txn_date, days_since_signup)
SELECT
tr.txn_id,
tr.customer_id,
tr.amount,
tr.txn_date,
CAST(julianday(tr.txn_date) - julianday(cr.signup_date) as INTEGER) as days_since_signup
FROM transactions_raw tr
LEFT JOIN customers_raw cr
ON tr.customer_id = cr.customer_id;
```

## Analytical Queries

### Monthly Revenue: total amount per month for the last 3 months

```sql
select
strftime('%Y-%m', txn_date) as Year_Month,
sum(amount)
from fact_transaction
where txn_date >= date(date('now', '-2 months'), 'start of month')
GROUP BY strftime('%Y-%m', txn_date)
ORDER BY Year_Month;
```

### New-Customer Spend: average spend within 30 days of signup

```sql
select avg(amount)
from fact_transaction
where days_since_signup < 30;
```

### Top 5 Customers: highest total spend in the last 90 days

```sql
with total_spend as (
select customer_id, sum(amount) as total_spend
from fact_transaction
where txn_date > date('now', '-90 days')
group by customer_id
), cte_ranked_spend as (
select customer_id, total_spend, dense_rank() over(order by total_spend desc) as ranked_spend
from total_spend
)
select customer_id, total_spend
from cte_ranked_spend
where ranked_spend <= 5;
```


