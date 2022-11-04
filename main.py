import asyncio

import pstats
import jmespath
import requests
import cProfile
import pstats
import pandas as pd
import aiohttp

from settings_request import headers, cookies


async def read_csv():
    df = pd.read_csv("requests.csv", sep=",", names=['Название запроса', 'Колличество', 'Название каталога'])
    request_name = df.head(100).iloc[:, 0].to_list()
    return request_name, df.head(100)


async def main():
    async with aiohttp.ClientSession() as session:
        csv_data = []
        queries, df = await read_csv()
        for query in queries:
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
                        csv_data.append('')
                    else:
                        csv_data.append('/'.join(data))
    df['Название каталога'] = csv_data
if __name__ == "__main__":
    with cProfile.Profile() as pr:
        
        asyncio.get_event_loop().run_until_complete(main())
    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.print_stats()
    # df.to_csv("/path/to/result.csv", index=False)
