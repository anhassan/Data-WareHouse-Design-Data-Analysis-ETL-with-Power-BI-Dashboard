import pandas as pd
import sqlite3, json, requests
import numpy as np
import logging
from pymongo import MongoClient
from dateutil import parser


# Enable logging level
logging.basicConfig(level = logging.INFO)

# Function to extract data from sqlite database
def extract_from_sqlite(Database, Table):
    sqlite_conn = sqlite3.connect(Database)
    sqlite_cursor = sqlite_conn.execute("SELECT * FROM {}".format(Table))
    rows = sqlite_cursor.fetchall()
    column_names = list(map(lambda x: x[0], sqlite_cursor.description))
    return pd.DataFrame(rows, columns=column_names)


# Function to extract data from mongodb database
def extract_from_mongodb(Database, Collection):
    pymongo_client = MongoClient('localhost', 27017)
    mongo_db = pymongo_client[Database]
    mongo_collection = mongo_db[Collection]
    return pd.DataFrame(mongo_collection.find()).drop(columns=["_id"])


# Function to extract data from csv file
def extract_from_csv(FileName):
    return pd.read_csv(FileName)


# Parsing json data from country based REST Api
def parse_api_response(country_name):
  get_request_url = "https://restcountries.eu/rest/v2/name/{}?fullText=true".format(country_name)
  response = requests.get(get_request_url)
  response_text = json.loads(response.text)[0]
  return [response_text["capital"], response_text["latlng"][0], response_text["latlng"][1]]


# Function to extract and transform location data
def extract_transform_location(sales_df, budget_df):
    location_list = list(set(list(budget_df["Location"].unique())
                             + list(sales_df["Location"].unique())))
    location_list = [location.lower() for location in location_list]
    location_table_list = [[index, country] + parse_api_response(country) for index, country in
                           enumerate(location_list)]
    transformed_location_data = pd.DataFrame(location_table_list,
                                             columns=["LocationId", "Country", "Capital", "Latitude", "Longitude"])
    return transformed_location_data


# Function to transform products data
def transform_product_data(products_df):
    transformed_products_data = products_df.copy()
    transformed_products_data.insert(0, "ProductId", np.arange(transformed_products_data.shape[0]))
    return transformed_products_data


# Function to transform(normalize) sales data
def transform_sales_data(sales_df, location_df, products_df):
    transformed_sales_df = sales_df.copy()
    transformed_sales_df["LocationId"] = transformed_sales_df["Location"].apply(lambda location: list(location_df[
                                                           location_df["Country"] == location.lower()]["LocationId"])[0])
    transformed_sales_df["ProductId"] = transformed_sales_df["Product"].apply(lambda product: list(products_df
                                                                [products_df["ProductName"] == product]["ProductId"])[0])
    transformed_sales_df.drop(['Location', 'Product'], axis=1, inplace=True)
    return transformed_sales_df


# Function to transform(normalize) budget data
def transform_budget_data(budget_df, location_df, products_df):
    transformed_budget_df = budget_df.copy()
    transformed_budget_df["LocationId"] = transformed_budget_df["Location"].apply(
        lambda location: list(location_df
                              [location_df["Country"] == location.lower()]["LocationId"])[0])
    transformed_budget_df["ProductId"] = transformed_budget_df["Product"].apply(
        lambda product: list(products_df
                             [products_df["ProductName"] == product]["ProductId"])[0])
    transformed_budget_df.drop(["Product", "ProductVendor", "Location"], axis=1, inplace=True)
    return transformed_budget_df


# Function to extract and transform date data
def extract_transform_date_data(sales_df):
    transformed_date_data = pd.DataFrame(sales_df["SalesDate"].values, columns=["DateId"])
    transformed_date_data["DateId"] = transformed_date_data["DateId"].apply(lambda date: parser.parse(date) )
    transformed_date_data["Day"] = transformed_date_data["DateId"].dt.day
    transformed_date_data['Month'] = transformed_date_data["DateId"].dt.month
    transformed_date_data['MonthName'] = transformed_date_data["DateId"].dt.strftime('%b')
    transformed_date_data["Year"] = transformed_date_data["DateId"].dt.year
    return transformed_date_data

# Function to load data in sqlite staging database
def load_data_sqlite(transformed_data, FileName, Database, Table):
    file_descriptor = open('Sql_Scripts/{}.sql'.format(FileName), 'r')
    sql_ddl = file_descriptor.read()
    file_descriptor.close()
    sqlite_conn = sqlite3.connect("{}.db".format(Database))
    sqlite_conn.execute(sql_ddl)
    transformed_data.to_sql(Table, sqlite_conn, if_exists='replace', index=False)


#-----------------------------------------------Extract Phase---------------------------------------------------------#

# Extracting data from sqlite database
logging.info("Extracting Sales Data from SqliteDB.....")
sales_df = extract_from_sqlite("SalesDatabase.db", "Sales")
# Extracting data from mongodb database
logging.info("Extracting Budget Data from MongoDB....")
budget_df = extract_from_mongodb("BudgetDatabase", "BudgetCollection")
# Extracting data from csv file
logging.info("Extracting Products Data from CSV......")
products_df = extract_from_csv("Products.csv")

#-----------------------------------------------Transform Phase--------------------------------------------------------#

# Extracting and Transfroming location data from country based REST Api [Transforming into Dimension Table]
logging.info("Transforming Location Data......")
transformed_location_data = extract_transform_location(sales_df, budget_df)
# Transforming products data [Introducing ProductId as Primary Key and Transforming into Dimension Table]
logging.info("Transforming Products Data......")
transformed_products_data = transform_product_data(products_df)
# Normalizing sales data [Transforming into Fact Table by using Foreign Keys]
logging.info("Transforming Sales Data......")
transformed_sales_data = transform_sales_data(sales_df, transformed_location_data, transformed_products_data)
# Normalizing budget data [Transforming into Fact Table by using Foreign Keys]
logging.info("Transforming Budget Data......")
transformed_budget_data = transform_budget_data(budget_df, transformed_location_data, transformed_products_data)
# Extracting and Transforming date data [Transforming into Dimension Table]
logging.info("Transforming Date Data......")
transformed_date_data = extract_transform_date_data(sales_df)

#-------------------------------------------------Load Phase---------------------------------------------------------#

# Loading Sales Fact Table in Sqlite Staging Database
logging.info("Loading Sales Data to StagingDB......")
load_data_sqlite(transformed_sales_data, "create_sales_fact", "StagingDatabase", "Sales")
# Loading Budget Fact Table in Sqlite Staging Database
logging.info("Loading Budget Data to StagingDB......")
load_data_sqlite(transformed_budget_data, "create_budget_fact", "StagingDatabase", "Budget")
# Loading Product Dimension Table in Sqlite Staging Database
logging.info("Loading Product Data to StagingDB......")
load_data_sqlite(transformed_products_data, "create_product_dim", "StagingDatabase", "Product")
# Loading Location Dimension Table in Sqlite Staging Database
logging.info("Loading Location Data to StagingDB......")
load_data_sqlite(transformed_location_data, "create_location_dim", "StagingDatabase", "Location")
# Loading Date Dimension Table in Sqlite Staging Database
logging.info("Loading Date Data to StagingDB......")
load_data_sqlite(transformed_date_data, "create_date_dim", "StagingDatabase", "DateDim")
