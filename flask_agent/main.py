#! /usr/bin/python
# -*- encoding: utf-8 -*-

import asyncio
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from utils.config import Settings
from utils.logger import yLogger
from agent import YandexAssistant
import eventlet

app = Flask(__name__, template_folder='templates')
settings = Settings(_env_file='.env', _env_file_encoding='utf-8')
logger = yLogger()
logger.initFile(settings.yandex.LOG_FILE_NAME)
app.config['SECRET_KEY'] = settings.yandex.SECRET_KEY
socketio = SocketIO(app, async_mode='eventlet')
WAIT_TIMEOUT = 30

@app.route('/')
def index():
	return render_template('./index.html')

@socketio.on('connected')
def conn(msg):
        return {'data':'Ok'}

@socketio.on('client_message')
def receive_message(data):
   socketio.emit('server_message', data, broadcast=True)
   thread = socketio.start_background_task(target=sync_wrapper_for_async, data=data, sid=request.sid)

def sync_wrapper_for_async(data, sid):
    """Wrapper to run the async function in its own event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(get_response(data, sid))
    loop.close()

async def get_response(input, sid):
   async with YandexAssistant(settings, sid) as assistant:
     try:
       resp = await asyncio.wait_for(assistant.one_shot(input.get('message')), timeout=WAIT_TIMEOUT)
     except asyncio.TimeoutError:
       resp = "Timeout occurred after waiting for " + WAIT_TIMEOUT + " seconds"
   socketio.emit('server_message', {'nickname': 'assistant', 'message': resp})

def send_update(data):
    socketio.emit('server_update_event', {'data': data})

if __name__ == "__main__":
    socketio.run(app, host='0.0.0.0', port=settings.yandex.PORT, debug=True)
