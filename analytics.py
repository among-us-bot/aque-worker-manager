"""
Created by Epic at 11/27/20
"""
from logging import getLogger
from aiohttp import ClientSession
from os import environ as env

analytics_url = f"https://{env['DELIVERY_HOST']}/api/:NAME:/:TYPE:"

logger = getLogger("analytics")


async def track(key: str, val: int, *, analytic_type="gauge"):
    async with ClientSession() as session:
        await session.post(analytics_url.replace(":NAME:", key).replace(":TYPE:", analytic_type),
                           headers={"value": val})
        logger.debug(f"Updating analytics for {key}!")
