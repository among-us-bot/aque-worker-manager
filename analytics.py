"""
Created by Epic at 11/27/20
"""
from logging import getLogger
from aiohttp import ClientSession
from os import environ as env
from collections import defaultdict
from time import time

analytics_url = f"http://{env['DELIVERY_HOST']}:5050/api/:NAME:/:TYPE:?value=:VALUE:"
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
        await session.post(analytics_url
                           .replace(":NAME:", key)
                           .replace(":TYPE:", analytic_type)
                           .replace(":VALUE:", str(val)))
        logger.debug(f"Updating analytics for {key}!")


async def histogram_push():
    global analytic_histogram_cache
    async with ClientSession() as session:
        for name, value in analytic_histogram_cache.items():
            await session.post(analytics_url
                               .replace(":NAME:", name)
                               .replace(":TYPE:", "histogram")
                               .replace(":VALUE:", str(value)))
        analytic_histogram_cache.clear()
        logger.debug("Updated all histogram analytics!")
