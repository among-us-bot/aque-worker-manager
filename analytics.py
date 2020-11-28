"""
Created by Epic at 11/27/20
"""
from logging import getLogger
from aiohttp import ClientSession
from asyncio import get_event_loop, sleep
from os import environ as env
from collections import defaultdict

analytics_url = f"http://{env['DELIVERY_HOST']}:5050/api/:NAME:/:TYPE:?value=:VALUE:"
analytic_histogram_cache = defaultdict(lambda: 0)

logger = getLogger("analytics")


async def track(key: str, val: int, *, analytic_type="gauge"):
    if analytic_type == "histogram":
        analytic_histogram_cache[key] += 1
        return
    async with ClientSession() as session:
        await session.post(analytics_url
                           .replace(":NAME:", key)
                           .replace(":TYPE:", analytic_type)
                           .replace(":VALUE:", str(val)))
        logger.debug(f"Updating analytics for {key}!")


async def histogram_push_loop():
    global analytic_histogram_cache
    async with ClientSession() as session:
        while True:
            for name, value in analytic_histogram_cache.items():
                await session.post(analytics_url
                                   .replace(":NAME:", name)
                                   .replace(":TYPE:", "histogram")
                                   .replace(":VALUE:", str(value)))
            analytic_histogram_cache.clear()
            logger.debug("Updated all histogram analytics!")
            await sleep(30)
loop = get_event_loop()
loop.create_task(histogram_push_loop())
