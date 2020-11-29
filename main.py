"""
Created by Epic at 11/1/20
"""
from color_format import basicConfig
from analytics import track, get_analytic

from os import environ as env
from asyncio import Lock
from aiohttp import WSMessage
from aiohttp.web import WebSocketResponse, WSMsgType, Application, run_app, get, Request, json_response
from ujson import loads, dumps
from logging import getLogger, DEBUG
from random import choice

global_logger = getLogger()
basicConfig(global_logger)
global_logger.setLevel(DEBUG)

logger = getLogger("worker-manager")

connected_workers = 0
connection_lock = Lock()
guild_workers = {}
workers = []
used_worker_ids = []

worker_descriptions = loads(env["WORKER_TOKENS"])


async def worker_connection(request):
    global connected_workers
    ws = WebSocketResponse()
    await ws.prepare(request)

    await connection_lock.acquire()
    msg: WSMessage
    unused_worker_ids = list(range(len(worker_descriptions)))
    for used_worker_id in used_worker_ids:
        unused_worker_ids.remove(used_worker_id)
    if len(unused_worker_ids) == 0:
        connection_lock.release()
        await ws.close(message=b"All tokens already assigned")
    worker_id = unused_worker_ids[0]
    used_worker_ids.append(worker_id)

    worker_info = {}
    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            data = msg.json(loads=loads)
            event_name = data["t"].lower()
            event_data = data["d"]

            if event_name == "add_guild":
                current_workers = guild_workers.get(event_data, [])
                current_workers.append(worker_info)
                guild_workers[event_data] = current_workers
            elif event_name == "remove_guild":
                guild_workers[event_data].remove(ws)
            elif event_name == "identify":
                worker_info = {
                    "name": worker_descriptions[worker_id]["name"],
                    "token": worker_descriptions[worker_id]["token"],
                    "ws": ws
                }
                workers.append(worker_info)
                logger.info(f"Worker with name {worker_info['name']} worker id {worker_id} identified, sending token!")
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
                await track(f"ratelimited_{event_data['guild']}", 1, analytic_type="histogram")
                logger.warning(f"Node {worker_info['name']} got rate-limited. Route: {event_data['route']}")
    connected_workers -= 1
    used_worker_ids.remove(worker_id)
    workers.remove(worker_info)
    logger.warning(f"Worker with name {worker_info} and worker id {worker_id} disconnected.")


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
                await track(f"worker_requests_{guild_id}", 1, analytic_type="histogram")
                available_workers = guild_workers.get(guild_id, None)
                if available_workers is None:
                    await track(f"not_enough_workers_{guild_id}", 1, analytic_type="histogram")
                    logger.warning(f"Guild \"{guild_id}\" has no active workers! Dismissing request")
                    continue
                worker = choice(available_workers)
                await worker["ws"].send_json(data, dumps=dumps)


async def query(request: Request):
    return json_response(await get_analytic(**request.query))


async def track_req(request: Request):
    q = request.query
    await track(q["key"], int(q["val"]), analytic_type=q["analytic_type"])
    return json_response({"status": "ok"})


app = Application()
app.add_routes([get("/workers", worker_connection), get("/controller", controller_connection), get("/query", query),
                get("/track", track_req)])
run_app(app, host="0.0.0.0", port=6060)
