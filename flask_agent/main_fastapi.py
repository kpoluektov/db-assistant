from fastapi import FastAPI
from pydantic import BaseModel
from utils.config import Settings
from utils.logger import yLogger
from agent import YandexAssistant

settings = Settings(_env_file='.env', _env_file_encoding='utf-8')
sessions = {}
assistant = YandexAssistant(settings, None)
class ChatRequest(BaseModel):
    session_id: str
    message: str

# 3. Create the Endpoint
@app.post("/chat")
async def chat_endpoint(request: ChatRequest):
    if request.session_id not in sessions:
        sessions[request.session_id] = SQLiteSession(session_id=request.session_id, db_path="chat.db")
    
    session = sessions[request.session_id]
    try:
        # Run the agent within the specific session context
        # This automatically handles message history for this session.
        result = await assistant.one_shot(request.message, session)
        
        return {
            "session_id": request.session_id,
            "response": result.final_output
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))