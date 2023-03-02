import bs4
import pandas as pd
import requests
import datetime as datetime
import re
import time
import datetime as datetime

## HELPER FUNCTIONS ##
def composition_clean(string):


    string = string.lower().strip().replace(',', '/').replace(' and ', '/').replace(u'\u2122', '').replace(u'\xae', '')

    return string.replace('(', '').replace(')', '').replace(' ', '-')

def replace_all(string):

    new_str = string.replace(u"\u2122", '')
    new_str = new_str.replace('-', '_').replace(' ', '_').replace('\'s', '') 

    return new_str

## Data Collection ##
def scrap(i, link_list):

    #time.sleep(1)
    try:
        urlt = 'https://www.macys.com' + link_list[i].find('a').get('href')

    except ConnectionError:
        print('Connection error')
        
    products = requests.get(urlt, headers=headers)
    soup_prod = bs4.BeautifulSoup(products.text, 'html.parser')
    
    # Products 
    products = soup_prod.find_all('li', class_=re.compile('cell productThumbnailItem'))
    i = 0

    products_data = pd.DataFrame()
    for prod in products:


        # PRODUCTS GENERAL INFORMATION
        product_id =  prod.find('div').get('id')


        name = prod.find('div', class_='productDescription').a.get('title')
        #print(name)

        discount = prod.find('div', class_='prices').find('span', class_='discount')
        
        if discount != None:

            price = discount.get_text().strip().replace('Now ', '').replace('Sale ', '')

        else:

            price = prod.find('div', class_='prices').span.get_text().strip()

        link = prod.find('div', class_='productDescription').a.get('href')

        category_id = re.search('CategoryID=(\d+)', link).group(1)

        if prod.find('div', class_=re.compile('bottomOverlayWrapper')) != None:

            category = prod.find('div', class_=re.compile('bottomOverlayWrapper')).div.get_text()
            
        else:

            category = None

        product_df = pd.DataFrame({'product_id': product_id,
                                   'product_name':name,
                                   'style_id':category_id,
                                   'product_type':category,
                                   'price':price,}, index=[0])


        # PRODUCTS DETAILS
        
        url_desc = 'https://www.macys.com' + link

        #time.sleep(0.5)
        description = requests.get(url_desc, headers=headers)

        soup_desc = bs4.BeautifulSoup(description.text, 'html.parser')

        
        # Color and Color ID

        colors = soup_desc.find('select', id='color-dropdown-for-sr')
        if colors != None and colors != []:

            colors = colors.find_all('option')
           
            soup_desc.find('select', id='color-dropdown-for-sr').find_all('option')[0].get('data-id')
            soup_desc.find('select', id='color-dropdown-for-sr').find_all('option')[0].get_text().strip()

            colors_ids = [color.get('data-id') for color in colors]
            colors_name = [color.get_text().strip() for color in colors]

        else:
           
            colors_ids = [None]
            colors_name = [None]

        if soup_desc.find_all('li', class_=re.compile('swatch-itm cell')) != []:

            size  = soup_desc.find_all('li', class_=re.compile('swatch-itm cell'))[0].get_text().strip()

        else:

            try:
                size = soup_desc.find_all('div', class_='sc-lbl')[0].span.get_text().strip()
            except:
                print('list out of index')
            finally:
                size=None
        # Composition
        details_list = soup_desc.find_all('div', class_='details-content')[0].find_all('li')
        for text in details_list:
            if text.get_text().__contains__('Cot'):
                composition = text.get_text()
            else:
                pass
        
        details_dic = {'product_id': product_id, 'color_id': colors_ids, 'color_name': colors_name, 'size': size, 'composition':composition}
        
        
        details = pd.DataFrame(details_dic, index=list(range(len(colors_ids))))
        
        
        product_df = product_df.merge(details, how='left', on='product_id')
       
        products_data = pd.concat([products_data, product_df], axis=0, ignore_index=True)

    return products_data
    

def collect_all(url, headers):


    url = 'https://www.macys.com/shop/mens-clothing/mens-jeans?id=11221&edge=hybrid'
    headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}

    result = requests.get(url, headers=headers)

    soup = bs4.BeautifulSoup(result.text, 'html.parser')


    general_categories_link = soup.find_all('div', class_='media-component media-component-category-icon')
    dfs = []

    for i in range(len(general_categories_link)):
        df = scrap(i, link_list=general_categories_link)
        dfs.append(df)

    df = pd.concat(dfs, axis=0)
    df['scrap_datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


    return df


def clean(products):

    df_man = products.copy()

    # Removing apostrophe and comma
    df_man['product_name'] = df_man['product_name'].apply(lambda x: x.split(',')[0] if x.__contains__(',') else x)
    df_man['product_name'] = df_man['product_name'].apply(lambda x: replace_all(x.strip()).lower())

 
    df_man['fit'] = df_man['product_name'].apply(lambda x: re.search('[a-z]+_fit', x).group(0) if x.__contains__('fit') else 'None')

    category = {43262: 'relaxed',
                240515: 'straight',
                43752: 'skinny',
                32808: 'slim'}
    
    df_man['style_id'] = df_man['style_id'].astype(int)

    df_man['product_type'] = df_man['style_id'].map(category)

    # df_man['color_id'] = df_man['color_id'].apply(lambda x: f'{x:.0f}' if pd.notnull(x) else x)

    df_man['color_name'] = df_man['color_name'].apply(lambda x: x.strip().replace(' -', '').replace('/ ', '/').replace(' ', '_').lower() if pd.notnull(x) else x)

    df_man['price'] = df_man['price'].apply(lambda x: float(x.strip().replace('USD ', '')))

    df_man['size'] = df_man['size'].apply(lambda x: x.replace('x', '/') if pd.notnull(x) else x)

    df_man['scrap_datetime'] = pd.to_datetime(df_man['scrap_datetime'], format='%Y-%m-%d %H:%M:%S')

    # Composition
    df_man['composition'] = df_man['composition'].apply(lambda x:  composition_clean(x))

    # Components Order
    df_aux = df_man['composition'].str.split('/', expand=True)
    df_aux.columns = [f'material_{i+1}' for i in range(df_aux.shape[1])]


    df_aux.loc[df_aux.material_3 == 'elastane-lycra', 'material_3'] = 'lycra-elastane'


    # Final DF
    df_cleaned = pd.concat([df_man, df_aux], axis=1)

    ordered_cols = ['product_id', 'style_id', 'color_id',  'product_type', 'product_name',  'color_name', 'fit', 'price', 'size', 'composition', 'material_1', 'material_2', 'material_3', 'material_4', 'material_5', 'scrap_datetime']

    df_final = df_cleaned[ordered_cols]

    df_final.to_csv('/home/brenoteix/repos/airflow/dags/src/data/raw/products_marcy.csv', index=False)

    return None


if __name__=='__main__':


    url = 'https://www.macys.com/shop/mens-clothing/mens-jeans?id=11221&edge=hybrid'
    headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}

    data = collect_all(url=url, headers=headers)

    clean(data)
