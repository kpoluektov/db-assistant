#! /usr/bin/python
# -*- encoding: utf-8 -*-

import asyncio
import json
import logging
import sys
import traceback
from contextlib import asynccontextmanager

import socketio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from agents.mcp import MCPServerSse
from utils.config import Settings
from utils.logger import yLogger
from utils.hooks import ExampleHooks
from agent import YandexAssistant, initialize_schema

_log = logging.getLogger("db_assistant")
_log.setLevel(logging.WARNING)
_log.addHandler(logging.StreamHandler(sys.stdout))

settings = Settings(_env_file='.env', _env_file_encoding='utf-8')
yLogger().initFile(settings.yandex.LOG_FILE_NAME)

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')


@asynccontextmanager
async def lifespan(app: FastAPI):
    await initialize_schema(settings)
    yield


app = FastAPI(lifespan=lifespan)
app.mount('/static', StaticFiles(directory='static'), name='static')
templates = Jinja2Templates(directory='templates')

socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

_tasks: set[asyncio.Task] = set()


class SqlRequest(BaseModel):
    sql: str


@app.get('/')
async def index(request: Request):
    return templates.TemplateResponse(request, 'index.html',
                                       headers={"Cache-Control": "no-store"})


@app.get('/api/sql-presets')
async def sql_presets():
    return settings.yandex.SQL_PRESETS


@app.post('/api/sql')
async def run_sql(body: SqlRequest):
    try:
        async with MCPServerSse(
            name="SQLRunner",
            params={"url": settings.yandex.GET_INFO_MCP_URL, "timeout": 60},
            cache_tools_list=False,
            client_session_timeout_seconds=30,
        ) as mcp:
            result = await mcp.call_tool("run_wide_sql", {"sql": body.sql})
        text = result.content[0].text
        try:
            rows = json.loads(text)
        except json.JSONDecodeError:
            return JSONResponse(status_code=400, content={"error": text})
        if not isinstance(rows, list):
            return JSONResponse(status_code=400, content={"error": text})
        return {"rows": rows, "count": len(rows)}
    except Exception as e:
        _log.error(f"SQL error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@sio.on('connected')
async def conn(sid, msg):
    return {'data': 'Ok'}


@sio.on('client_message')
async def receive_message(sid, data):
    await sio.emit('server_message', data)
    task = asyncio.create_task(_handle_message(sid, data))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def _handle_message(sid: str, data: dict) -> None:
    try:
        hooks = ExampleHooks(sio=sio, sid=sid)
        async with YandexAssistant(settings, sid, hooks=hooks, sio=sio) as assistant:
            resp = await assistant.one_shot(data.get('message'))
    except Exception as e:
        _log.error(f"Agent error: {e}\n{traceback.format_exc()}")
        resp = f"Error: {e}"
    await sio.emit('server_message', {'nickname': 'assistant', 'message': resp})


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(socket_app, host='0.0.0.0', port=settings.yandex.PORT)
