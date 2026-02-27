from agents import Agent, RunContextWrapper, RunHooks, Tool, Usage
from agents.items import ModelResponse, TResponseInputItem
from typing import Optional, Any
import logging

class ExampleHooks(RunHooks):
    def __init__(self):
        self.event_counter = 0
        self.logger = logging.getLogger("openai.agents")

    def _usage_to_str(self, usage: Usage) -> str:
        return f"{usage.requests} requests, {usage.input_tokens} input tokens, {usage.output_tokens} output tokens, {usage.total_tokens} total tokens"

    async def on_agent_start(self, context: RunContextWrapper, agent: Agent) -> None:
        self.event_counter += 1
        self.logger.debug(
            f"### {self.event_counter}: Agent {agent.name} started. Usage: {self._usage_to_str(context.usage)}"
        )

    async def on_llm_start(
        self,
        context: RunContextWrapper,
        agent: Agent,
        system_prompt: Optional[str],
        input_items: list[TResponseInputItem],
    ) -> None:
        self.event_counter += 1
        self.logger.debug(f"### {self.event_counter}: LLM started. with {input_items}.")

    async def on_llm_end(
        self, context: RunContextWrapper, agent: Agent, response: ModelResponse
    ) -> None:
        self.event_counter += 1
        self.logger.debug(f"### {self.event_counter}: LLM ended.")

    async def on_agent_end(self, context: RunContextWrapper, agent: Agent, output: Any) -> None:
        self.event_counter += 1
        self.logger.debug(
            f"### {self.event_counter}: Agent {agent.name} ended with output {output}. Usage: {self._usage_to_str(context.usage)}"
        )

    async def on_tool_start(self, context: RunContextWrapper, agent: Agent, tool: Tool) -> None:
        self.event_counter += 1
        self.logger.debug(
            f"### {self.event_counter}: Tool {tool.name} started. context is {context}"
        )

    async def on_tool_end(
        self, context: RunContextWrapper, agent: Agent, tool: Tool, result: str
    ) -> None:
        self.event_counter += 1
        self.logger.debug(
            f"### {self.event_counter}: Tool {tool.name} ended with result {result}. "
        )

    async def on_handoff(
        self, context: RunContextWrapper, from_agent: Agent, to_agent: Agent
    ) -> None:
        self.event_counter += 1
        self.logger.debug(
            f"### {self.event_counter}: Handoff from {from_agent.name} to {to_agent.name}. "
        )

hooks = ExampleHooks()