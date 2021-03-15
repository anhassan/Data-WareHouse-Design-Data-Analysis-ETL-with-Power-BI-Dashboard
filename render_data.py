import pandas as pd
import datetime, random
import json,logging
import sqlite3
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient

# Enable logging level
logging.basicConfig(level = logging.INFO)

# Configuration Parameters
START_DATE = datetime.datetime(2018, 1, 1)
NUM_MONTHS, NUM_DAYS = 24, 730
MIN_BUDGET_AMT, MAX_BUDGET_AMT = 1000, 100000
MIN_SALES_AMT, MAX_SALES_AMT = 100, 10000

# Product and Location Categories
transaction_locations = ["USA", "UK", "Canada", "Australia", "Austria", "Argentina", "India", "China", "Brazil",
                         "Turkey"]
product_vendors = ["Sony", "Amazon", "Samsung", "Apple", "Tesla", "Shopify", "Spotify", "Uber", "Microsoft", "Facebook"]
product_category = ["Technology", "E-Commerce", "Technology", "Technology", "Vehicle", "E-Commerce", "Social Media",
                    "Vehicle"
    , "Technology", "Social Media"]


# Function to render Budget Data
def create_budget_data(min_amount, max_amount, starting_date, num_months):
    budget_transactions = [[str(starting_date + relativedelta(months=num)), random.randint(min_amount, max_amount),
                            "Product{}".format((prod_num) + 1), product_vendors[prod_num],
                            transaction_locations[random.randint(0, len(transaction_locations) - 1)]]
                           for prod_num in range(0, len(product_vendors)) for num in range(0, num_months)]
    budget_transactions_df = pd.DataFrame(budget_transactions,
                                          columns=["BudgetDate", "BudgetAmt", "Product", "ProductVendor", "Location"])
    return budget_transactions_df


# Function to render Sales Data
def create_sales_data(min_amount, max_amount, starting_date, num_days):
    sales_transactions = [[starting_date + relativedelta(days=num), random.randint(min_amount, max_amount),
                           "Product{}".format(prod_num + 1),
                           transaction_locations[random.randint(0, len(transaction_locations) - 1)]]
                          for prod_num in range(0, len(product_vendors)) for num in range(0, num_days)]
    sales_transactions_df = pd.DataFrame(sales_transactions, columns=["SalesDate", "SalesAmt", "Product", "Location"])
    return sales_transactions_df


# Function to render Product Data
def create_product_data(product_vendors, product_category):
    products = [["Product{}".format(index + 1), product_vendors[index], product_category[index]] for index in
                range(0, len(product_vendors))]
    products_df = pd.DataFrame(products, columns=['ProductName', "ProductVendor", "ProductCategory"])
    return products_df


# Pushing Budget Data to MongoDB

# Creating Database and Collection in MongoDB
pymongo_client = MongoClient('localhost', 27017)
mongo_db = pymongo_client["BudgetDatabase"]
mongo_collection = mongo_db["BudgetCollection"]

# Getting Budget Documents from Budget Data Renderer
budget_df = create_budget_data(MIN_BUDGET_AMT, MAX_BUDGET_AMT, START_DATE, NUM_MONTHS)
budget_documents = json.loads(budget_df.to_json(orient="records"))

# Inserting Documents in bulk in MongoDB Collection
logging.info("Pushing Data to MongoDB.......")
mongo_collection.insert_many(budget_documents)

# Pushing Sales Data to SqliteDB

# Creating a Database and Table in Sqlite
file_descriptor = open('Sql_Scripts/create_sales_table.sql', 'r')
sql_ddl = file_descriptor.read()
file_descriptor.close()
sqlite_conn = sqlite3.connect("SalesDatabase.db")
sqlite_conn.execute(sql_ddl)

# Getting Sales Transaction from Sales Data Renderer
sales_df = create_sales_data(MIN_SALES_AMT, MAX_SALES_AMT, START_DATE, NUM_DAYS)

# Inserting Sales Rows in bulk in Sqlite Table
logging.info("Pushing Data to SqliteDB.......")
sales_df.to_sql('Sales', sqlite_conn, if_exists='replace', index=False)

# Pushing Products Data to CSV[Comma Seperated Value] file

# Getting Products Data from Product Data Renderer
products_df = create_product_data(product_vendors, product_category)

# Inserting Product Rows in bulk in CSV file periodically
logging.info("Pushing Data to CSV............")
products_df.to_csv("Products.csv", encoding='utf-8', index=False)
