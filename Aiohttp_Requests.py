"""
Use this to send many HTTP requests from given IDs in ForceManager.
In this case we want to download the "Documents" given a list of Accounts IDs.
- Paolo Frazzetto paolofraz@gmail.com

Useful references:
https://developer.forcemanager.com/
https://blog.jonlu.ca/posts/async-python-http
https://realpython.com/async-io-python/#a-full-program-asynchronous-requests
"""
import asyncio
import sys
import time
from pathlib import Path

import aiohttp
import nest_asyncio
import pandas as pd
from asyncio_throttle import Throttler
from tqdm.asyncio import tqdm

nest_asyncio.apply()
# Use private libraries to load API tokens
path_utils = Path("../../QstAnalysis/amajor-qst/amajor_qst").resolve()
sys.path.append(str(path_utils))
from AmajorUtilities import credentials

token, response = credentials()

PARALLEL_REQUESTS = 200  # Adjust according to plan

if __name__ == '__main__':
    # Get data, set up headers, connections and storing structure
    headers = {'Content-Type': 'application/json', 'Accept': '*/*', 'X-Session-Key': token}
    Accounts = pd.read_excel("./data/some_data.xlsx")  # File exported from FM with Accounts IDs column
    d = {}  # Store responses as {AccountID: Resp}
    conn = aiohttp.TCPConnector(limit=PARALLEL_REQUESTS / 2, limit_per_host=PARALLEL_REQUESTS / 2, use_dns_cache=False,
                                force_close=True)

    start = time.perf_counter()


    async def gather_with_throttler(n):
        throttler = Throttler(rate_limit=n, period=65, retry_interval=5)  # Limit API calls every 65 seconds
        async with aiohttp.ClientSession(base_url="https://api.forcemanager.com", headers=headers,
                                         connector=conn) as session:
            # here is the logic for the generator
            async def fetch(idx):
                async with throttler:
                    # Define API URL and filters
                    async with session.get(
                            f"/api/v4/accounts/{idx}/documents?where=extension NOT LIKE 'jpg' AND extension NOT LIKE 'png'") as request:
                        assert request.status == 200, f" Request status: {request.status} for ID: {idx}"
                        obj = await request.json()
                        d[idx] = obj

            await tqdm.gather(*(fetch(idx) for idx in Accounts.loc[:, "Account ID"]))


    loop = asyncio.get_event_loop()
    loop.run_until_complete(gather_with_throttler(PARALLEL_REQUESTS))
    end = time.perf_counter()

    # Save results
    Accounts["_Request"] = Accounts["Account ID"].map(d)
    Accounts.to_excel("./data/preprocess_requests.xlsx")
    # print(d)

    print(f"Completed {len(d)} requests in {end - start:.2f} seconds")
