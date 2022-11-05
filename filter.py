import asyncio
from multiprocessing import cpu_count, Pool
import json

import pstats
import jmespath
import cProfile
import pstats
import pandas as pd
import aiohttp
import numpy as np

from settings_request import headers, cookies


def read_csv():
    df = pd.read_csv("filter.csv", sep=",", names=['Название запроса', 'Колличество', 'Название каталога'])
    request_name = df.iloc[:, 0].to_list()
    return request_name, df

async def get_data(query):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'https://card.wb.ru/cards/detail?spp=0&regions=80,64,83,4,38,33,70,82,69,68,86,75,30,40,48,1,22,66,31,71&pricemarginCoeff=1.0&reg=0&appType=1&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1029256,-102269,-2162196,-1257786&nm={query}', headers=headers) as response:
                response = await response.json(content_type=None)
                subject = jmespath.search('data.products[0].subjectId', response)
                kind = jmespath.search('data.products[0].kindId', response)
                brand = jmespath.search('data.products[0].brandId', response)
                id = jmespath.search('data.products[0].id', response)
                params = {
                    'subject': f'{subject}',
                    'kind': f'{kind}',
                    'brand': f'{brand}',
                }
                async with session.get(f'https://www.wildberries.ru/webapi/product/{id}/data', params=params, cookies=cookies,
                                        headers=headers) as response:
                    response = await response.json(content_type=None)
                    data = jmespath.search('value.data.sitePath[:-1].name', response)
                    if data == None:
                        data = ''
                    else:
                        data = '/'.join(data)
    except:
        data = ''
    return data

def start(query):
    data = asyncio.get_event_loop().run_until_complete(get_data(query))
    return data
# def is_float(x):
#     try:
#         float(x)
#     except ValueError:
#         return False
#     return True
def main():
    # df = pd.read_csv("requests.csv", sep=",", names=['Название запроса', 'Колличество', 'Название каталога'])
    # df = df[df['Название запроса'].apply(lambda x: is_float(x))]
    # df.to_csv("filter.csv", index=False)
    # with cProfile.Profile() as pr:
    queries, df = read_csv()
    records = [(query,) for query in queries]
    with Pool(cpu_count()) as p:
        data = p.starmap(start, records)
        p.close()
        p.join()
    df['Название каталога'] = data
    df.to_csv("result_filter.csv", index=False)    
    # stats = pstats.Stats(pr)
    # stats.sort_stats(pstats.SortKey.TIME)
    # stats.print_stats()
    
if __name__ == "__main__":
    main()
        