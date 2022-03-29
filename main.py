import asyncio
import pandas as pd
import sys
import requests
import os

ranked_stats_url = 'https://api.niftygateway.com//market/ranked-stats/'

worker_count = 30
nifty_responses = []
final_nifties = []

async def fetch(nifty):

    """
    Get stats for given nifty object
    :param nifty: Nifty json object
    :return:
    """
 
    contract_name = nifty["unminted_nifty_obj"]["niftyTitle"]
    nifty_name = nifty["unminted_nifty_obj"]["niftyTitle"]
    nifty_date_created = nifty["date_created"]
    nifty_date_modified = nifty["date_modified"]
    num_pm_sales = nifty["number_of_pm_sales"]
    num_sm_sales = nifty["number_of_sm_sales"]
    original_price = nifty['orig_price_in_cents']
    avg_resale_price = nifty["average_secondary_market_sale_price_in_cents"]

    sm_market_volume = nifty["sum_of_primary_market_sales_in_cents"]

    nifty_obj_dict = {

        "contract_name": contract_name,
        "nifty_name": nifty_name,
        "nifty_date_created": nifty_date_created,
        "nifty_date_modified": nifty_date_modified,
        "num_pm_sales": num_pm_sales,
        "num_sm_sales": num_sm_sales,
        "avg_resale_price": avg_resale_price,
        'original_price' : original_price,

        "sm_market_volume": sm_market_volume,

    }
    # Append dictionary to nifties list
    final_nifties.append(nifty_obj_dict)


async def worker(queue):
    """
    Asynchronous worker that receives items from queue
    :param queue:
    :return:
    """
    print('START WORKER')
    while True:
        nifty = await queue.get()
        await fetch(nifty)
        queue.task_done()

        if len(final_nifties) == len(nifty_responses):
            print('len(nifty_responses): ', len(nifty_responses))
            print('Starting Dataframe preparation')
            df = pd.DataFrame(final_nifties)
            df['nifty_date_created'] = pd.to_datetime(df['nifty_date_created'])
            df.sort_values(by = 'nifty_date_created')

            dir_path = os.path.dirname(os.path.realpath(__file__))

            file_path = dir_path+'/Q2_results.csv'
            df.to_csv(file_path)


            """ I am using "nifty_date_created" as a proxy for date of primary sale."""

            df = df[(df['nifty_date_created'] >= '2021-10-13')]

            df = df[(df['nifty_date_created'] <= '2021-10-20')]

            df['nifty_date_created'] = [str(i)[:10] for i in df['nifty_date_created']]
            df['sales_volume(USD)'] = df['num_pm_sales'] * df['original_price']
            df['sales_volume(USD)'] = df['sales_volume(USD)']/100

            df_new = df.groupby('nifty_date_created').sum()[["sales_volume(USD)", "num_pm_sales"]]

            d = df_new.dtypes
            df_new.loc['Total'] = df_new.sum(numeric_only=True)
            df_new.astype(d)

            df_new = df_new.rename(columns = { "num_pm_sales": "num_primary_market_sales"})

            file_path = dir_path+'/Q2_results_filtered.csv'
            df_new.to_csv(file_path)

            print('TASK COMPLETE')
            sys.exit()
        

async def control(queue):
    """
    Control mechanism for asynchronous queue.
    :param queue:
    :return:
    """
    for nifty in nifty_responses:
        queue.put_nowait(nifty)
        await asyncio.sleep(0.02)

async def main():
    await get_nifties()
    queue = asyncio.Queue()
    await asyncio.gather(
        control(queue),
        asyncio.gather(*[worker(queue) for _ in range(worker_count)])
    )

async def get_nifties():
    global nifty_responses

    page = 1
    while True:
        url = ranked_stats_url+'?'+'size=100&'+'current='+'{}'.format(page)+'&ranking_type=number_of_pm_sales&order_by=asc'
        r = requests.get(url)

        response_json = r.json()

        # print('response_json: ',response_json)
        response_json_updated = response_json["data"]["results"]

        nifty_responses.extend(response_json_updated)
        if response_json["data"]["meta"]["page"]["total_pages"] == page:
        # if 10 == page:
            break
        else:
            page += 1
            print('status: {}/{}'.format(page,response_json["data"]["meta"]["page"]["total_pages"]))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
