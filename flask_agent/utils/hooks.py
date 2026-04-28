from agents import Agent, RunContextWrapper, RunHooks, Tool
from agents.items import ModelResponse, TResponseInputItem
from typing import Optional, Any
import logging
import socketio


class ExampleHooks(RunHooks):
    def __init__(self, sio: socketio.AsyncServer, sid: str):
        self.logger = logging.getLogger("openai.agents")
        self._sio = sio
        self._sid = sid

    async def _emit(self, step_type: str, **kwargs) -> None:
        await self._sio.emit('agent_step', {'type': step_type, **kwargs}, to=self._sid)

    async def on_agent_start(self, context: RunContextWrapper, agent: Agent) -> None:
        await self._emit('agent_start', agent=agent.name)

    async def on_llm_start(
        self,
        context: RunContextWrapper,
        agent: Agent,
        system_prompt: Optional[str],
        input_items: list[TResponseInputItem],
    ) -> None:
        await self._emit('llm_start', agent=agent.name)

    async def on_llm_end(
        self, context: RunContextWrapper, agent: Agent, response: ModelResponse
    ) -> None:
        await self._emit('llm_end', agent=agent.name)

    async def on_agent_end(self, context: RunContextWrapper, agent: Agent, output: Any) -> None:
        await self._emit('agent_end', agent=agent.name)

    async def on_tool_start(self, context: RunContextWrapper, agent: Agent, tool: Tool) -> None:
        await self._emit('tool_start', tool=tool.name)

    async def on_tool_end(
        self, context: RunContextWrapper, agent: Agent, tool: Tool, result: str
    ) -> None:
        await self._emit('tool_end', tool=tool.name)

    async def on_handoff(
        self, context: RunContextWrapper, from_agent: Agent, to_agent: Agent
    ) -> None:
        await self._emit('handoff', from_agent=from_agent.name, to_agent=to_agent.name)
