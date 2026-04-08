from agents import Agent, RunContextWrapper, RunHooks, Tool, Usage
from agents.items import ModelResponse, TResponseInputItem
from typing import Optional, Any
import logging


class ExampleHooks(RunHooks):
    def __init__(self, step_queue=None, sid=None):
        self.logger = logging.getLogger("openai.agents")
        self._step_queue = step_queue

    def _emit(self, step_type: str, **kwargs):
        if self._step_queue is not None:
            self._step_queue.put({'type': step_type, **kwargs})

    async def on_agent_start(self, context: RunContextWrapper, agent: Agent) -> None:
        self._emit('agent_start', agent=agent.name)

    async def on_llm_start(
        self,
        context: RunContextWrapper,
        agent: Agent,
        system_prompt: Optional[str],
        input_items: list[TResponseInputItem],
    ) -> None:
        self._emit('llm_start', agent=agent.name)

    async def on_llm_end(
        self, context: RunContextWrapper, agent: Agent, response: ModelResponse
    ) -> None:
        self._emit('llm_end', agent=agent.name)

    async def on_agent_end(self, context: RunContextWrapper, agent: Agent, output: Any) -> None:
        self._emit('agent_end', agent=agent.name)

    async def on_tool_start(self, context: RunContextWrapper, agent: Agent, tool: Tool) -> None:
        self._emit('tool_start', tool=tool.name)

    async def on_tool_end(
        self, context: RunContextWrapper, agent: Agent, tool: Tool, result: str
    ) -> None:
        self._emit('tool_end', tool=tool.name)

    async def on_handoff(
        self, context: RunContextWrapper, from_agent: Agent, to_agent: Agent
    ) -> None:
        self._emit('handoff', from_agent=from_agent.name, to_agent=to_agent.name)

