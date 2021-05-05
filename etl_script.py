import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine

username = 'Dagadt' #Please enter username
password = 'W4r381!17' #Please enter password

engine = create_engine('mysql+pymysql://{}:{}@localhost/random_sales_data_db'.format(username, password),
                       connect_args= dict(host='localhost', port=3306))
conn = engine.connect()

class Etl():
    def __init__(self, csv_filepath):
        self.df = pd.read_csv(csv_filepath, skiprows=3)
        
    
    def transform_data(self):
        #Shape dataframe
        self.df.columns = self.df.iloc[0]
        self.df = self.df.drop(0)
        self.df = self.df.rename(columns=str).rename(columns={'nan':'new_lbl'})
        self.df = self.df.drop('new_lbl', axis=1).drop(self.df.tail(2).index)
        
        #Rename Columns
        self.df = self.df.rename(columns={ 
                                    'RegionTerritoryID':'territory_id',
                                    'RegionTerritoryName':'territory_name',
                                    'SalesGroupName':'sales_group_name',
                                    'ProductName':'product_name',
                                    'SKU':'sku',
                                    'Lot':'lot',
                                    'Qty':'qty',
                                    'SalesPrice':'sales_price_$',
                                    'SalesDate':'sales_date',
                                    'SalesTime':'sales_time',
                                    'SalesPersonID':'sales_person_id',
                                    'SalesPerson':'sales_person',
                                    'TransactionId':'transaction_id',
                                    'ProductId':'product_id',
                                    'SalesGroupId':'sales_group_id'
                                })
        
        #Change columns data type
        self.df['territory_id'] = self.df['territory_id'].astype(int) #ok
        self.df['territory_name'] = self.df['territory_name'].astype(str) #ok
        self.df['sales_group_id'] = self.df['sales_group_id'].astype(str)
        self.df['sales_group_name'] = self.df['sales_group_name'].astype(str)
        self.df['product_id'] = self.df['product_id'].astype(str)
        self.df['product_name'] = self.df['product_name'].astype(str)
        self.df['sku'] = self.df['sku'].astype(str)
        self.df['lot'] = self.df['lot'].astype(str)
        self.df['transaction_id'] = self.df['transaction_id'].astype(str)
        self.df['qty'] = self.df['qty'].astype(int)
        self.df['sales_price_$'] = self.df['sales_price_$'].str.replace('$', '').astype(float)
        self.df['sales_date'] = pd.to_datetime(self.df['sales_date'])
        self.df['sales_time'] = pd.to_datetime(self.df['sales_time'])
        self.df['sales_time'] = [time.time() for time in self.df['sales_time']]
        self.df['sales_person_id'] = self.df['sales_person_id'].astype(str)
        self.df['sales_person'] = self.df['sales_person'].astype(str)
        
        
        #Create tables
        #Create territory table
        self.territory_table_df = self.df[['territory_id', 'territory_name']].drop_duplicates(subset = ['territory_id'])
        
        #Create sales group table
        self.sales_group_table_df = self.df[['sales_group_id', 'sales_group_name', 'sales_person_id']].drop_duplicates(subset = ['sales_group_id'])
        
        #Create product table
        self.product_table_df = self.df[['product_id', 'sku', 'lot', 'product_name']].drop_duplicates(subset = ['product_id'])
        
        #Create transaction table
        self.transaction_table_df = self.df[['transaction_id', 'qty', 'sales_price_$', 'qty', 'sales_price_$',
                                            'sales_date', 'sales_time',  'sales_person_id', 'product_id']]
        #Create sales person table
        self.sales_person_table_df = self.df[['sales_person_id', 'sales_person',  'territory_id']]
        
        return self.territory_table_df, self.product_table_df, self.sales_group_table_df, self.transaction_table_df, self.sales_person_table_df
    
    def load_data(self):
        self.territory_table_df.to_sql('territory', con=conn, if_exists='append', index=False)
        self.sales_group_table_df.to_sql('sales_group', con=conn, if_exists='append', index=False)
        self.product_table_df.to_sql('product', con=conn, if_exists='append', index=False)
        self.transaction_table_df.to_sql('transactions', con=conn, if_exists='append', index=False)
        self.sales_person_table_df.to_sql('sales_person', con=conn, if_exists='append', index=False)


if __name__ == '__main__':
    data = Etl('datasets//RandomSalesData.csv')
    data.transform_data()
    data.load_data()