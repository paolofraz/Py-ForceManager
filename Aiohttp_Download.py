"""
Use this file to download "documents" form ForceManager.
All the links have been previously scraped from aiohttp requests and saved into a file.
- Paolo Frazzetto poalofraz@gmail.com

Useful references:
https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html
"""
import asyncio
import os
import time
from pathlib import Path

import aiofiles
import aiohttp
import nest_asyncio
import pandas as pd
from asyncio_throttle import Throttler
from tqdm.asyncio import tqdm

nest_asyncio.apply()
# Use private libraries to load API tokens
import credentials

token, response = credentials()

PARALLEL_REQUESTS = 240
DOWNLOAD_DIR = Path("./Attachments")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':

    # Get data, set up headers, connections and storing structure
    headers = {'Content-Type': 'application/json', 'Accept': '*/*', 'X-Session-Key': token}
    Docs = pd.read_excel("./data/test_attach.xlsx", index_col=0)  # File exported from FM and processed with Docs links

    start = time.perf_counter()


    async def download_with_throttler(n):
        throttler = Throttler(rate_limit=n, period=65, retry_interval=5)  # Limit API calls every 65 seconds
        session = aiohttp.ClientSession(raise_for_status=True, headers=headers)

        async def fetch_account(index, row):
            async with throttler:
                async def download_file(url: str, filename: object = None,
                                        download_path: object = DOWNLOAD_DIR) -> object:
                    if url:  # Check whether there is something to download
                        local_filename = url.split('/')[-1].split('?')[0]
                        if filename:
                            _, extension = os.path.splitext(local_filename)
                            filename += extension
                        else:
                            filename = local_filename
                        download_path = Path(download_path, filename)
                        if download_path.exists():
                            # print("File exists")
                            return
                        async with session.get(url) as resp:
                            async with aiofiles.open(download_path, mode='wb') as f:
                                data = await resp.read()
                                await f.write(data)

                await download_file(row["link"], str(index))

        await tqdm.gather(*(fetch_account(index, row) for index, row in Docs.iterrows()))
        await session.close()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_with_throttler(PARALLEL_REQUESTS))
    end = time.perf_counter()

    time_elapsed = end - start
    print("Downloaded {} documents.".format(len(Docs)))
    print(
        "Download Time Elapsed: {:02d}:{:02d}:{:02d}".format(int(time_elapsed // 3600), int(time_elapsed % 3600 // 60),
                                                             int(time_elapsed % 60 // 1)))
