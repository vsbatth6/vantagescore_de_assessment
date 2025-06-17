import os
import sys
import logging
import pandas as pd
import sqlite3
from datetime import datetime



# Source the data from CSV files and read into pandas dataframes
def source_data(cust_data, trans_data):
	cust_df = pd.read_csv(cust_data)
	txn_df = pd.read_csv(trans_data)
	logger.info("Data sourced successfully")

	return cust_df, txn_df

# Create databse and staging table for task 1
def create_db(database_name):
	conn = sqlite3.connect(database_name)
	cursor = conn.cursor()


	cursor.execute(
		"""
		CREATE TABLE IF NOT EXISTS stg_transactions (
			txn_id INTEGER PRIAMRY KEY,
			customer_id INTEGER,
			txn_Date DATE,
			amount REAL,
			signup_date DATE,
			days_since_signup INTEGER
			)
		"""
	)

	cursor.close()
	conn.commit()
	conn.close()


def transform_Data(cust_df, txn_df):
	# Convert date columns to datetime for calculations

	# Exit script if signup_date conversion to timestamp fails
	try:
		cust_df["signup_date"] = pd.to_datetime(cust_df["signup_date"])
	except Exception as e:
		logger.error("Error in Dataframe: customers")
		logger.error(f"Error converting signup_Date to datetime: {e}")
		sys.exit(1)

	# Exit script if txn_date conversion to timestamp fails
	try:
		txn_df["txn_date"] = pd.to_datetime(txn_df["txn_date"])
	except Exception as e:
		logger.error("Error in Dataframe: transactions")
		logger.error(f"Error converting txn_date to datetime: {e}")
		sys.exit(1)

	# Drop transactions older than 90 days
	cutoff_date = datetime.now() - pd.Timedelta(days=90)
	txn_df_filtered = txn_df[txn_df["txn_date"] >= cutoff_date]

	# Merge datasets on customer_id, using transactions as the left table
	# transactions table is left table because a customer may not have any transactions
	# but a transaction must have a customer
	stg_transactions_df = pd.merge(txn_df_filtered, cust_df, on="customer_id", how="left")

	# Calcualte days_since_signup attribute
	stg_transactions_df["days_since_signup"] = (stg_transactions_df["txn_date"] - stg_transactions_df["signup_date"]).dt.days

	# Select attributes and order for staging table
	stg_transactions_df = stg_transactions_df[["txn_id", "customer_id", "txn_date", "amount", "signup_date", "days_since_signup"]]

	# Convert timestamp fields to date for table loading
	stg_transactions_df["txn_date"] = stg_transactions_df["txn_date"].dt.date
	stg_transactions_df["signup_date"] = stg_transactions_df["signup_date"].dt.date

	return stg_transactions_df


def load_tables(database_name, table_name, data):
	conn = sqlite3.connect(database_name)
	logger.info(f"Connected to {database_name} for data loading")

	logger.info("Starting data load")
	# Load table with if_exists="replace" to overwrite existing data dna dprevent duplicates
	data.to_sql(table_name, conn, if_exists="replace", index=False)
	logger.info(f"Data loaded into table: {table_name}")

	# Validate table load row count
	cursor = conn.cursor()
	cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
	row_count = cursor.fetchone()[0]
	logger.info(f"Number of rows in table {table_name}: {row_count}")

	# Validate table load sample records
	cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
	sample_records = cursor.fetchall()
	logger.info(f"Sample data from table {table_name}:")

	column_names = [description[0] for description in cursor.description]
	logger.info(column_names)
	for records in sample_records:
		logger.info(records)

	cursor.close()
	conn.commit()
	conn.close()




if __name__ == "__main__":
	# Configure logging to save toa  timestamped file
	if not os.path.exists("logs"):
		os.makedirs("logs")
	logging.basicConfig(level=logging.INFO,
					 format="%(filename)s - %(asctime)s - %(levelname)s - %(message)s",
					 filename=f"logs/etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log",
					 filemode="w")
	logger = logging.getLogger()

	# Start ETL process
	logger.info("Starting ETL pipeline")
	customer_dataset = "customers.csv"
	transactions_dataset = "transactions.csv"
	database_name = "VirinderpalSinghBatth_DE_Assessment.db"

	# Check if the dataset files exist
	if os.path.exists(customer_dataset) and os.path.exists(transactions_dataset):
		logger.info("Data files found. Starting data sourcing")
		cust_df, txn_df = source_data(customer_dataset, transactions_dataset)
	else:
		logger.info("Data files not found")
		logger.info("Exiting script")
		sys.exit(1)

	# If the databse does not exist, then create it
	if not os.path.exists(database_name):
		logger.info("Starting database creation")
		create_db(database_name)
		logger.info("Database created succesfully")
	else:
		logger.info("Database already exists. Skipping creation.")

	# Transform the data
	logger.info("Starting data transformation")
	stg_transactions_df = transform_Data(cust_df, txn_df)
	logger.info("Data tranformed successfuly")

	if stg_transactions_df.empty:
		logger.info("No data to load after transformation. Exiting Script")
		sys.exit(1)
	else:
		load_tables(database_name, "stg_transactions", stg_transactions_df)

	logger.info("ETL Pipeline compeleted")


