from bs4 import BeautifulSoup
import requests
import warnings
warnings.simplefilter(action='ignore')
import pandas as pd
import time
from loguru import logger

logger.add('Aerobase_Store_Data_extract.log', rotation='500 MB')

product_urls = []
title = []
part_number = []
manufacture_brand = []
cost = []
category = []
subcategory = []
short_describe = []
long_description = []
country_of_origin = []
unit_of_measure = []
no_of_unit = []

try:
    start1 = time.time()
    for i in range(1,21):
        print(f'Enter page {i}')
        nav_url = f'https://aerobase.store/aircraft+lighting/page/{i}'
        source = requests.get(nav_url).text
        soup = BeautifulSoup(source, 'html.parser')

        product_items = soup.find_all('div', class_='abg-product-list-item')
        for item in product_items:
            try:
                product_url = 'https://aerobase.store/' + item.find('a', href=True)['href']
                product_urls.append(product_url)
                product_name = item.find('a', class_='font-normal').text.strip()
                title.append(product_name)
                mpn = item.find('strong', text='MPN: ').next_sibling.strip()
                part_number.append(mpn)
                manufacturer = item.find('strong', text='Manufacturer: ').next_sibling.strip()
                manufacture_brand.append(manufacturer)
                price = item.find('span', class_='price').text.strip()
                cost.append(price)
                logger.info(f"Successfully completed to collect the basic Information of the {product_name} of page{i} ...\n")
            except Exception as ex1:
                print(f"Skipping the item's basic information due to missing value : {ex1}")
                logger.info(f"Extracting Data missing due to insufficient {product_name}'s information ... \n\n")

        print(f'Completed page {i}')
        logger.info(f"Successfully completed Extracting all Product's basic Information of Page{i} ...")
except Exception as e1:
    print(e1)
finally:
    end1 = time.time()
    elapsed_time1 = end1 - start1
    print(f"For getting all data from web, time taken was : {elapsed_time1}ms ...")
# print(product_urls)

try:
    start2 = time.time()
    for purl in product_urls:
        print(f"Enter for data extraction to this site \n --> {purl}")
        

        try:
            psource = requests.get(purl)
            psource.raise_for_status()
            psource_text = psource.text
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occured ... {e}")
            logger.info(f"HTTP error occured ... >>>> {e} for this Page ...")
            continue

        psoup = BeautifulSoup(psource_text, 'html.parser')

        try:
            product_define = psoup.find('div', id='breadcrumbs').find_all('a', class_='a')
            category_1 = product_define[-2].text.strip()
            category.append(category_1)
            subcategory_2 = product_define[-1].text.strip()
            subcategory.append(subcategory_2)
        except Exception as ex2:
            print(f"Skipping product's Category information, due to missing data : {ex2}")
            logger.info("Skipping product's Category information, due to missing breadcrumbs")
            category.append('N/A')
            subcategory.append('N/A')

        try:
            short_description = psoup.find('span', class_='font11em').text.strip()
            short_describe.append(short_description)
        except Exception as ex3:
            print(f"Skipping product's short_description information, due to missing data : {ex3}")
            logger.info(f"Skipping product's short description information, due to missing data ... {ex3}")
            short_describe.append('N/A')

        try:
            deep_describe = psoup.find('div', class_='panel-body')
            table = deep_describe.find('table', class_='table table-body table-noborder table-bold table-respond margin-top font09em')
            body = table.find('tbody')
            long_desc = []
            for row in body.find_all('tr'):
                row_data = []
                for col in row.find_all('td'):
                    row_data.append(col.text.strip())
                long_desc.append(row_data)
            country = long_desc[len(long_desc)-1:]
            long_description.append(dict(long_desc))
            country_of_origin.append(country[0][1])
        except Exception as ex4:
                print(f"Skipping product's long_description information, due to missing data : {ex4}")
                logger.info(f"Skipping product's long description information, due to missing data ... {ex4}")
                long_description.append('N/A')
                country_of_origin.append('N/A')

        try:
            measureing_unit = psoup.find('span', class_='small block italic').text.strip()
            unit_of_measure.append(measureing_unit)
            if measureing_unit.lower() in ['each', 'box']:
                num_per_unit_measure = 1
                no_of_unit.append(num_per_unit_measure)
            else:
                no_of_unit.append('N/A')
        except Exception as ex5:
                print(f"Skipping product's measureing_unit information, due to missing data : {ex5}")
                logger.info(f"Skipping product's measuring unit's information, due to missing data ... {ex5}")
                unit_of_measure.append('N/A')            

        print('Completed task ...')
        logger.info(f"Completed all data extraction to this site -->> {purl}")
        # status = 'Successful'
except Exception as e2:
    print(e2)
    # status = 'Failure'
finally:
    end2 = time.time()
    elapsed_time2 = end2 - start2
    print(f"For getting all data from web, time taken was : {elapsed_time2}ms ...")

start3 = time.time()
list_length = min(len(part_number), len(title), len(short_describe), len(long_description), len(manufacture_brand), len(cost), len(country_of_origin), len(unit_of_measure), len(no_of_unit), len(category), len(subcategory))

part_number = part_number[:list_length]
title = title[:list_length]
short_describe = short_describe[:list_length]
long_description = long_description[:list_length]
manufacture_brand = manufacture_brand[:list_length]
cost = cost[:list_length]
country_of_origin = country_of_origin[:list_length]
unit_of_measure = unit_of_measure[:list_length]
no_of_unit = no_of_unit[:list_length]
category = category[:list_length]
subcategory = subcategory[:list_length]


data_dict = {
    'Number' : part_number,
    'Product Name/Title' : title,
    'Short Description' : short_describe,
    'Long Description' : long_description,
    'Manufacturer' : manufacture_brand,
    'Brand' : manufacture_brand,
    'List Price' : cost,
    'Country of Origin' : country_of_origin,
    'Unit of Measure' : unit_of_measure,
    'Measure' : no_of_unit,
    'Category 1' : category,
    'SUBCategory 2' : subcategory
}

df = pd.DataFrame(data_dict)
print(df.sample(5))
print('Converting DataFrame to Excel File ...')
logger.info('Converting DataFrame to Excel File ...')
df.to_excel('aerobase_data_final.xlsx', index=False)
print('Successfully written the .xlsx file ...')
logger.info('Successfully written the .xlsx file ...')

end3 = time.time()
elapsed_time3 = end3 - start3
extraction_time = elapsed_time1 + elapsed_time2
total_time = extraction_time + elapsed_time3
logger.info(f"Total Extraction time : {extraction_time}ms")
logger.info(f"Total Processing time : {total_time}ms")
logger.info(f"\n\n\n\~~~~~~~~~~~~~~~~~~~  Finally Complete the Overall Process at : {time.time()}  ~~~~~~~~~~~~~~~~~~~\n\n\n\n\n")