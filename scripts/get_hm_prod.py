import re
import requests
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from datetime import datetime


# Data Collection
def data_collection(url, headers):

    # Url request
    page = requests.get(url, headers=headers)

    # BeautifulSoup Object
    soup_i = BeautifulSoup(page.text, 'html.parser')

    # Getting the pagination to show all the products in the page
    product_total = int(soup_i.find('h2', class_='load-more-heading').get('data-total'))
    pags = np.ceil(product_total/36)

    # Url with all the products
    url1 = url + '?offset=0&page-size=' + str(round(pags*36))

    # New BeastifulSoup Object
    page_t = requests.get(url1, headers=headers)
    soup = BeautifulSoup(page_t.text, 'html.parser')

    #=#=#=#=#=#=#=#Collecting The Products in the vitrine=#=#=#=#=#=#=#=#=#

    products = soup.find('ul', class_='products-listing small')
    # List wwith all the products
    prod_list = products.find_all('article', class_='hm-product-item')

    # Products ID
    products_id = [p.get('data-articlecode')  for p in prod_list]

    # Products Type
    products_type = [p.get('data-category') for p in prod_list]

    # Products Names
    products_n_list = products.find_all('a', class_='link')
    products_names = [p.get_text() for p  in products_n_list]

    # Prices
    price_list = products.find_all('span', class_='price regular')
    products_prices = [p.get_text() for p in  price_list]

    # DataFrame of the Products
    products_df = pd.DataFrame([products_id, products_type, products_names, products_prices]).T
    products_df['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    products_df.columns=['product_id', 'product_type', 'product_name', 'price', 'scrap_datetime']
    products_df['style_id'] = products_df['product_id'].apply(lambda x: x[:-3])


    # Removing the products with the same Style ID
    products_df = products_df[~products_df.duplicated('style_id')]

    return products_df


# Data Collection by Product
def data_collection_by_product(products_df, headers):
    aux_list = []
    cols = ['product_id', 'Composition', 'Fit', 'Size']
    # DataFrame 
    df_pattern = pd.DataFrame(columns=cols)
    # DataFrame for all compositions
    df_comp_final = pd.DataFrame()
    # DataFrame for all colors
    df_colors_final = pd.DataFrame()

    #aux_list = []

    for i in products_df['product_id']:

        # Getting the link of each product

        url_p = 'https://www2.hm.com/'+ f'en_us/productpage.{i}.html'

        page_p = requests.get(url_p, headers=headers)

        soup_prod = BeautifulSoup(page_p.text, 'html.parser')

        # Using regular expression to find the class with the product colors.
        prod_att = soup_prod.find_all('a', class_=re.compile('^filter-option miniature'))
        colors = [p.get('data-color') for p in prod_att]
        prod_id = [p.get('data-articlecode') for p in prod_att]
        
        # Getting the colors of the product
        df_colors = pd.DataFrame({'product_id': prod_id, 'color_name': colors})
        
        df_colors_final = pd.concat([df_colors_final, df_colors], axis=0)
        
        # Product Composition
        for p in soup_prod.find_all('a', class_=re.compile('^filter-option miniature')):
        
            url = 'https://www2.hm.com/'+p.get('href')

            
            comps_page = requests.get(url, headers=headers)
            comps_soup = BeautifulSoup(comps_page.text, 'html.parser')

        
            attributes = comps_soup.find('div', class_='content pdp-text pdp-content').find_all('div')
        
            # Using the split() to make a list with the strings and filtering the blank values
            composition = [list(filter(None, p.get_text().split('\n'))) for p in attributes]
            df = pd.DataFrame(composition).T
        
            # Using the first row as the header
    
            data_comp = df.rename(columns=df.iloc[0]).drop(df.index[0])
            data_comp = data_comp.rename(columns={'Art. No.': 'product_id'})
            data_comp = data_comp.fillna(method='ffill')
            
            df_comp_final = pd.concat([df_comp_final, data_comp], axis=0)
            
            # Keep new columns tha show up
            aux_list = aux_list + data_comp.columns.to_list()
            
    # Garantee of the pattern of the dataset
    df_colors_final['style_id'] = df_colors_final['product_id'].apply(lambda x: x[:-3])
    df_colors_final['color_id'] = df_colors_final['product_id'].apply(lambda x: x[-3:])

    df_comp_final['style_id'] = df_comp_final['product_id'].apply(lambda x: x[:-3])
    df_comp_final['color_id'] = df_comp_final['product_id'].apply(lambda x: x[-3:])

    # Merging the colors with the composition, fit and size
    df_final = df_colors_final.merge(df_comp_final[['product_id', 'Fit', 'Composition', 'Size']], how='left', on='product_id')

    raw_data = products_df.merge(df_final[['product_id', 'style_id', 'color_id','color_name', 'Fit', 'Composition', 'Size']], how='left', on='style_id')

    return raw_data

# Data Cleaning
def data_cleaning(data):

    # Using the unique products id's
    data['product_id_x'] = data['product_id_y']
    data = data.drop(columns='product_id_y')
    data.rename(columns={'product_id_x': 'product_id'}, inplace=True)

        
    # product_name
    data['product_name'] = data['product_name'].apply(lambda x: x.replace(' ', '_').lower())    

    # price
    data['price'] = data['price'].apply(lambda x: float(x.replace('$ ', '')))         
    # scrap_datetime
    data['scrap_datetime'] = pd.to_datetime(data['scrap_datetime'], format='%Y-%m-%d %H:%M:%S') 
    
    # color_name
    data['color_name'] = data['color_name'].apply(lambda x: x.replace(' ', '_').replace('/', '_').lower())

    # Fit
    data['Fit'] = data['Fit'].apply(lambda x: x.replace(' ', '_').lower())        

    # Size 
    # Extracting only the height of the model
    data['model_height'] = data['Size'].apply(lambda x: re.search('\d{3}', x).group(0) if pd.notnull(x) else x)
    # Extracting only the size of the model
    data['model_size'] = data['Size'].str.extract('(\d+/\d+)')

    products = data.copy()

    # Removing unecessary information from the composition column.
    products['Composition'] = data['Composition'].str.replace('Shell: ', '').str.replace('Pocket lining:', '').str.replace('Lining: ', '').str.replace('Pocket: ', '')


    df = products['Composition'].str.split(',', expand=True)

    # Creating a DF to use as a reference to the composition data.
    df_ref = pd.DataFrame(index=np.arange(len(df)),columns=['ref'])


    # Making columns for each element in the composition.
    # Cotton
    cotton = df.loc[df[0].str.contains('Cotton', na=True)][0]
    cotton_2 = df.loc[df[1].str.contains('Cotton', na=True)][1]

    # Combine the two series
    cotton = cotton.combine_first(cotton_2)
    cotton.name = 'cotton'

    df_ref = pd.concat([df_ref, cotton], axis=1)

    # Rayon
    rayon = df.loc[df[0].str.contains('Rayon', na=True)][0]
    rayon_2 = df.loc[df[2].str.contains('Rayon', na=True)][2]
    rayon = rayon.combine_first(rayon_2)
    rayon.name = 'rayon'
    df_ref = pd.concat([df_ref, rayon], axis=1)

    # Lyocell
    lyocell = df.loc[df[0].str.contains('Lyocell', na=True)][0]
    lyocell_2 = df.loc[df[1].str.contains('Lyocell', na=True)][1]
    lyocell = lyocell.combine_first(lyocell_2)
    lyocell.name = 'lyocell'

    df_ref = pd.concat([df_ref, lyocell], axis=1)


    # Polyester
    polyester = df.loc[df[0].str.contains('Polyester', na=True)][0]
    polyester_2 = df.loc[df[1].str.contains('Polyester', na=True)][1]
    polyester = polyester.combine_first(polyester_2)
    polyester.name = 'polyester'

    df_ref = pd.concat([df_ref, polyester], axis=1)

    # Spandex
    spandex = df.loc[df[1].str.contains('Spandex', na=True)][1]
    spandex_2 = df.loc[df[2].str.contains('Spandex', na=True)][2]
    spandex = spandex.combine_first(spandex_2)
    spandex.name = 'spandex'

    df_ref = pd.concat([df_ref, spandex], axis=1)

    # Elastomultiester
    elastomultiester = df.loc[df[1].str.contains('Elastomultiester', na=True)][1]
    elastomultiester.name = 'elastomultiester'
    df_ref = pd.concat([df_ref, elastomultiester], axis=1)


    # Composition data 
    df_comp = df_ref.drop(columns='ref')

    # Selecting the percentage of each compnent.
    df_comp = df_comp.applymap(lambda x: int(re.search('\d+', x).group(0))/100 if pd.notnull(x) else x).fillna(0)

    # Dataframe auxilar -> Get rid of the product_id duplicates
    df_aux = pd.concat([products[['product_id']], df_comp], axis=1)
    df_aux = df_aux.groupby('product_id').max()

    # Final Dataset
    products = pd.merge(products, df_aux, how='left', on='product_id')
    products = products.drop(columns=['Composition', 'Size']).drop_duplicates()
    products.columns = [c.lower() for c in products.columns.to_list()]

    products.to_csv('/home/brenoteix/repos/airflow/dags/src/data/raw/products_hm.csv', index=False)

    return None


if __name__ == '__main__':
  
    # parameters and constants
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'}

    # URL
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'
    
    # data collection
    data = data_collection(url, headers)

    # data collection by product
    data_product = data_collection_by_product(data, headers)

    # data cleaning
    data_cleaning(data_product)
