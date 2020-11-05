"""
Created by Epic at 11/1/20
"""
from color_format import basicConfig

from os import environ as env
from asyncio import Lock
from aiohttp import WSMessage
from aiohttp.web import WebSocketResponse, WSMsgType, Application, run_app, get
from ujson import loads, dumps
from logging import getLogger, DEBUG
from random import choice

logger = getLogger("worker-manager")
basicConfig(logger)
logger.setLevel(DEBUG)

connected_workers = 0
connection_lock = Lock()
guild_workers = {}
workers = []

worker_descriptions = loads(env["WORKER_TOKENS"])


async def worker_connection(request):
    global connected_workers
    ws = WebSocketResponse()
    await ws.prepare(request)

    await connection_lock.acquire()
    msg: WSMessage
    connected_workers += 1
    connection_num = connected_workers
    worker_info = {}
    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            data = msg.json(loads=loads)
            event_name = data["t"].lower()
            event_data = data["d"]

            if event_name == "add_guild":
                current_workers = guild_workers.get(event_data, [])
                current_workers.append(ws)
                guild_workers[event_data] = current_workers
            elif event_name == "remove_guild":
                guild_workers[event_data].remove(ws)
            elif event_name == "identify":
                worker_info = {
                    "name": worker_descriptions[connection_num - 1]["name"],
                    "token": worker_descriptions[connection_num - 1]["token"],
                    "ws": ws
                }
                workers.append(worker_info)
                logger.info(f"Worker with name {worker_info['name']} identified, sending token!")
                await ws.send_json({
                    "t": "dispatch_bot_info",
                    "d": {
                        "name": worker_info["name"],
                        "token": worker_info["token"]
                    }
                })
                logger.info("Sent token!")
                connection_lock.release()
            elif event_name == "ratelimit":
                logger.warning(f"Node {worker_info['name']} got rate-limited. Route: {event_data}")
    connected_workers -= 1
    workers.remove(worker_info)


async def controller_connection(request):
    ws = WebSocketResponse()
    await ws.prepare(request)

    msg: WSMessage
    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            data = msg.json(loads=loads)
            event_name = data["t"].lower()
            event_data = data["d"]

            if event_name == "request":
                guild_id = event_data["guild_id"]
                available_workers = guild_workers[guild_id]
                worker = choice(available_workers)
                await worker["ws"].send_json(data, dumps=dumps)

app = Application()
app.add_routes([get("/workers", worker_connection), get("/controller", controller_connection)])
run_app(app, host="0.0.0.0", port=6060)
