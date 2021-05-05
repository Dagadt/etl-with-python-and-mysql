import mysql.connector
import pandas as pd


host = 'localhost' 
user = '' # Please replace UserName with your user name
password = '' # Please replace Password with your password

query = ''' 
            DROP DATABASE IF EXISTS random_sales_data_db;
            CREATE DATABASE random_sales_data_db;
            USE random_sales_data_db;
            
            CREATE TABLE region (
                region_id VARCHAR(255),
                client_region_id VARCHAR(255),
                region_name VARCHAR(255),
                PRIMARY KEY (region_id)
            );
            
            CREATE TABLE product (
                product_id VARCHAR(255),
                sku VARCHAR(255),
                lot VARCHAR(255),
                product_name VARCHAR(255),
                PRIMARY KEY (product_id)
            );

            CREATE TABLE sales_person (
                sales_person_id VARCHAR(255),
                client_sales_person_id VARCHAR(255),
                sales_person_name VARCHAR(255),
                territory_id VARCHAR(255),
                PRIMARY KEY (sales_person_id),
                FOREIGN KEY (territory_id) REFERENCES territory(territory_id)
            );
            
            CREATE TABLE territory (
                territory_id VARCHAR(255),
                client_territory_id VARCHAR(255),
                territory_name VARCHAR(255),
                region_id VARCHAR(255),
                PRIMARY KEY (territory_id),
                FOREIGN KEY (region_id) REFERENCES region(region_id)
            );

            CREATE TABLE transactions (
                transaction_id VARCHAR(255),
                transaction_date DATE,
                sales_person_id VARCHAR(255),
                product_id VARCHAR(255),
                qty INT(32),
                sales_price_$ FLOAT(32),
                is_return BOOL,
                PRIMARY KEY (transaction_id),
                FOREIGN KEY (sales_person_id) REFERENCES sales_person(sales_person_id),
                FOREIGN KEY (product_id) REFERENCES product(product_id)
            );
                
            CREATE TABLE sales_group (
                sales_group_id VARCHAR(255),
                sales_group_name VARCHAR(255),
                sales_person_id VARCHAR(255),
                region_id VARCHAR(255),
                PRIMARY KEY (sales_group_id),
                FOREIGN KEY (sales_person_id) REFERENCES sales_person(sales_person_id),
                FOREIGN KEY (region_id) REFERENCES region(region_id)
            );
            '''

def create_db_connection(host, user, password):
    mydb = mysql.connector.connect(
      host=host,
      user=user,
      password=password
    )
    
    return mydb

def run_query(query, connection):
    with connection.cursor() as cursor:
            cursor.execute(query)
    

connection = create_db_connection(host, user, password)
run_query(query, connection)
print('Run successfully')

