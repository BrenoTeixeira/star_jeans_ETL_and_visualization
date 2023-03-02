import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Data Insertion
def data_insert_hm(data_products, connection_params):

    data_insert = data_products[['product_id',
        'style_id',
        'color_id',
        'product_type',
        'product_name',
        'color_name',
        'fit',
        'price',
        'model_size',
        'model_height',
        'cotton',
        'rayon',	
        'lyocell',	
        'polyester',	
        'spandex',
        'elastomultiester',
        'scrap_datetime']]

    db = create_engine(connection_params)
    con = db.connect()
    data_insert.to_sql('vitrine_hm', con=con, if_exists='append', index=False)


def data_insert_marcy(data_products, connection_params):

    data_insert = data_products[['product_id',
    'style_id',
    'color_id',
    'product_type',
    'product_name',
    'color_name',
    'fit',
    'price',
    'size',
    'composition',
    'material_1',
    'material_2',
    'material_3', 
    'material_4',
    'material_5',
    'scrap_datetime']]

    db = create_engine(connection_params)
    con = db.connect()
    
    data_insert.to_sql('vitrine_marcy', con=con, if_exists='append', index=False)
 

if __name__ == '__main__':

    con_postgres = PostgresHook(postgres_conn_id='postgres_local_conn').get_uri()

    try:
        data_product_cleaned_hm = pd.read_csv('/home/brenoteix/repos/airflow/dags/src/data/raw/products_hm.csv')
        # data insertion
        data_insert_hm(data_product_cleaned_hm, con_postgres)

    except FileNotFoundError:
        print('File not found. Check if the file exists.')

    try:
        products_marcy = pd.read_csv('/home/brenoteix/repos/airflow/dags/src/data/raw/products_marcy.csv')

        data_insert_marcy(products_marcy, con_postgres)

    except FileNotFoundError:
        print('File not found. Check if the file exists.')