"""
Created by Epic at 11/27/20
"""
from logging import getLogger
from aiohttp import ClientSession
from os import environ as env
from collections import defaultdict
from time import time

analytics_base_url = f"http://{env['DELIVERY_HOST']}:5050"
analytics_url = f"{analytics_base_url}/api/:NAME:/:TYPE:?value=:VALUE:"
analytics_query_url = f"{analytics_base_url}/api/query?query=:QUERY:&start=:START:&end=:END:&step=14"
analytic_histogram_cache = defaultdict(lambda: 0)
last_push = time()

logger = getLogger("analytics")


async def track(key: str, val: int, *, analytic_type="gauge"):
    global last_push
    if analytic_type == "histogram":
        analytic_histogram_cache[key] += 1
        logger.debug(f"Updating analytics for {key}!")
        if time() >= last_push + 30:
            logger.debug("Starting histogram analytics update!")
            last_push = time()
            await histogram_push()
        return
    async with ClientSession() as session:
        r = await session.post(analytics_url
                               .replace(":NAME:", key)
                               .replace(":TYPE:", analytic_type)
                               .replace(":VALUE:", str(val)))
        r.raise_for_status()
        logger.debug(f"Updating analytics for {key}!")


async def get_analytic(query, start, end):
    async with ClientSession() as session:
        r = await session.get(analytics_query_url
                              .replace(":QUERY:", query)
                              .replace(":START:", start)
                              .replace(":END:", end))
        return await r.json()


async def histogram_push():
    global analytic_histogram_cache
    async with ClientSession() as session:
        for name, value in analytic_histogram_cache.items():
            r = await session.post(analytics_url
                                   .replace(":NAME:", name)
                                   .replace(":TYPE:", "histogram")
                                   .replace(":VALUE:", str(value)))
            r.raise_for_status()
        analytic_histogram_cache.clear()
        logger.debug("Updated all histogram analytics!")
