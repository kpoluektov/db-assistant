#! /usr/bin/python
# -*- encoding: utf-8 -*-

import asyncio
import logging
import sys
import traceback
import queue as _stdlib_queue
from flask import Flask, render_template, request
from flask_socketio import SocketIO
from utils.config import Settings
from utils.logger import yLogger
from utils.hooks import ExampleHooks
from agent import YandexAssistant, initialize_schema
import eventlet
import eventlet.tpool

_log = logging.getLogger("db_assistant")
_log.setLevel(logging.WARNING)
_log.addHandler(logging.StreamHandler(sys.stdout))

def _suppress_closed_loop(loop, context):
    exc = context.get('exception')
    if isinstance(exc, RuntimeError) and 'Event loop is closed' in str(exc):
        return
    loop.default_exception_handler(context)

app = Flask(__name__, template_folder='templates')
settings = Settings(_env_file='.env', _env_file_encoding='utf-8')
logger = yLogger()
logger.initFile(settings.yandex.LOG_FILE_NAME)
app.config['SECRET_KEY'] = settings.yandex.SECRET_KEY
socketio = SocketIO(app, async_mode='eventlet')

@app.route('/')
def index():
	return render_template('./index.html')

@socketio.on('connected')
def conn(msg):
        return {'data': 'Ok'}

@socketio.on('client_message')
def receive_message(data):
    socketio.emit('server_message', data, broadcast=True)
    socketio.start_background_task(target=_handle_message, data=data, sid=request.sid)

def _handle_message(data, sid):
    # Bridge for step events: asyncio thread → eventlet greenlet → socketio
    step_queue = _stdlib_queue.Queue()

    def step_emitter():
        while True:
            try:
                item = step_queue.get(block=False)
                if item is None:
                    break
                socketio.emit('agent_step', item, room=sid)
            except _stdlib_queue.Empty:
                eventlet.sleep(0.05)

    emitter = eventlet.spawn(step_emitter)
    try:
        # tpool runs the asyncio loop in a real OS thread so the eventlet hub
        # stays alive — WebSocket heartbeats continue during long agent runs.
        resp = eventlet.tpool.execute(_run_agent_in_thread, data, sid, step_queue)
    except Exception as e:
        _log.error(f"Agent error: {e}\n{traceback.format_exc()}")
        resp = f"Error: {e}"
    finally:
        step_queue.put(None)
        emitter.wait()

    socketio.emit('server_message', {'nickname': 'assistant', 'message': resp})

def _run_agent_in_thread(data, sid, step_queue):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(_suppress_closed_loop)
    try:
        return loop.run_until_complete(_get_agent_response(data, sid, step_queue))
    finally:
        loop.close()

async def _get_agent_response(data, sid, step_queue):
    resp = "No response from assistant"
    try:
        hooks = ExampleHooks(step_queue=step_queue, sid=sid)
        async with YandexAssistant(settings, sid, hooks=hooks) as assistant:
            resp = await assistant.one_shot(data.get('message'))
    except Exception as e:
        _log.error(f"Agent response error: {e}\n{traceback.format_exc()}")
        resp = f"Error: {e}"
    return resp

def _run_init_in_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(initialize_schema(settings))
    except Exception as e:
        _log.error(f"Schema initialization failed: {e}")
    finally:
        loop.close()

if __name__ == "__main__":
    import threading
    t = threading.Thread(target=_run_init_in_thread, daemon=False)
    t.start()
    t.join()
    socketio.run(app, host='0.0.0.0', port=settings.yandex.PORT, debug=False)
