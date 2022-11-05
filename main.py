import asyncio
from multiprocessing import cpu_count, Pool

import pstats
import jmespath
import cProfile
import pstats
import pandas as pd
import aiohttp

from settings_request import headers, cookies


def read_csv():
    df = pd.read_csv("requests.csv", sep=",", names=['Название запроса', 'Колличество', 'Название каталога'])
    request_name = df.iloc[:, 0].to_list()
    return request_name, df

async def get_data(query):
    try:
        async with aiohttp.ClientSession() as session:
            query = query.replace(' ', '+')
            async with session.get(
                f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=12,3,18,15,'
                f'21&curr=rub&dest=-1029256,-102269,-2162196,'
                f'-1257786&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={query}&reg=0&regions=80,64,83,4,38,33,'
                f'70,82,69,68,86,75,30,40,48,1,22,66,31,'
                f'71&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
                headers=headers) as response:
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

def main():
    # with cProfile.Profile() as pr:
    queries, df = read_csv()
    records = [(query,) for query in queries]
    with Pool(cpu_count()) as p:
        data = p.starmap(start, records)
        p.close()
        p.join()
    df['Название каталога'] = data
    df.to_csv("result.csv", index=False)    
    # stats = pstats.Stats(pr)
    # stats.sort_stats(pstats.SortKey.TIME)
    # stats.print_stats()
    
if __name__ == "__main__":
    main()
        
    
    
